"""Glue Job 2 — transformation_job.py (PySpark / Glue ETL 4.0).

Loads raw stream events, enriches them with song metadata, computes the genre
level daily KPIs, ranks the top songs and genres, and writes the result to the
processed bucket as partitioned Parquet.

Runtime : AWS Glue ETL (PySpark), Glue version 4.0.
Input parameters (Glue job arguments, passed by Step Functions):
    --raw_bucket        name of the raw S3 bucket
    --reference_bucket  name of the reference S3 bucket
    --processed_bucket  name of the processed S3 bucket
    --quarantine_bucket name of the S3 bucket for quarantined (invalid) rows
    --JOB_NAME          standard Glue parameter (required for GlueContext init)

The transformation logic lives in small, pure functions that accept and return
Spark DataFrames so they can be unit tested with a local SparkSession (no
GlueContext, no AWS). Only ``main`` touches GlueContext.

Data quality is enforced before KPI calculations via ``tag_invalid_rows``:
    1. Null values in any required column (user_id, track_id, listen_time).
    2. Blank/whitespace-only values in user_id or track_id.
    3. Non-numeric user_id (cannot be cast to integer).
    4. Non-positive user_id (must be > 0).
    5. Unparseable listen_time (cannot be cast to timestamp).
    6. Duplicate rows (same user_id + track_id + listen_time).
Invalid rows are written to s3://<quarantine_bucket>/quarantine/ as Parquet
with a ``_quarantine_reason`` column explaining why each row was rejected.
"""

from functools import reduce

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, TimestampType

REQUIRED_STREAM_COLUMNS = ["user_id", "track_id", "listen_time"]


# ---------------------------------------------------------------------------
# Data quality / quarantine
# ---------------------------------------------------------------------------

def tag_invalid_rows(streams_df):
    """Classify raw stream rows into clean and quarantined sets.

    Runs six checks in order; a row is quarantined on the first check it fails.
    The quarantine DataFrame carries a ``_quarantine_reason`` StringType column
    describing exactly why each row was rejected.

    Checks (applied sequentially so early failures don't obscure later ones):
        1. Null values in user_id, track_id, or listen_time.
        2. Blank/whitespace-only user_id or track_id.
        3. Non-numeric user_id (cannot cast to IntegerType).
        4. Non-positive user_id (value <= 0 after cast).
        5. Unparseable listen_time (cannot cast to TimestampType).
        6. Duplicate rows sharing the same (user_id, track_id, listen_time).

    Returns:
        (clean_df, quarantine_df)  — two non-overlapping DataFrames.
        quarantine_df is None when every row passes.
    """
    quarantine_parts = []
    clean = streams_df

    # --- Check 1: Null required columns ---
    null_mask = (
        F.col("user_id").isNull()
        | F.col("track_id").isNull()
        | F.col("listen_time").isNull()
    )
    null_rows = clean.filter(null_mask).withColumn(
        "_quarantine_reason",
        F.concat_ws(
            ", ",
            F.when(F.col("user_id").isNull(), F.lit("null user_id")),
            F.when(F.col("track_id").isNull(), F.lit("null track_id")),
            F.when(F.col("listen_time").isNull(), F.lit("null listen_time")),
        ),
    )
    if null_rows.count() > 0:
        quarantine_parts.append(null_rows)
    clean = clean.filter(~null_mask)

    # --- Check 2: Blank/whitespace user_id or track_id ---
    blank_mask = (F.trim(F.col("user_id")) == "") | (F.trim(F.col("track_id")) == "")
    blank_rows = clean.filter(blank_mask).withColumn(
        "_quarantine_reason",
        F.concat_ws(
            ", ",
            F.when(F.trim(F.col("user_id")) == "", F.lit("blank user_id")),
            F.when(F.trim(F.col("track_id")) == "", F.lit("blank track_id")),
        ),
    )
    if blank_rows.count() > 0:
        quarantine_parts.append(blank_rows)
    clean = clean.filter(~blank_mask)

    # --- Check 3: Non-numeric user_id ---
    non_numeric_mask = F.col("user_id").cast(IntegerType()).isNull()
    non_numeric_rows = clean.filter(non_numeric_mask).withColumn(
        "_quarantine_reason",
        F.lit("non-numeric user_id: cannot be cast to integer"),
    )
    if non_numeric_rows.count() > 0:
        quarantine_parts.append(non_numeric_rows)
    clean = clean.filter(~non_numeric_mask)

    # --- Check 4: Non-positive user_id (zero or negative integer) ---
    non_positive_mask = F.col("user_id").cast(IntegerType()) <= 0
    non_positive_rows = clean.filter(non_positive_mask).withColumn(
        "_quarantine_reason",
        F.concat(
            F.lit("non-positive user_id: value is "),
            F.col("user_id"),
            F.lit(" (must be > 0)"),
        ),
    )
    if non_positive_rows.count() > 0:
        quarantine_parts.append(non_positive_rows)
    clean = clean.filter(~non_positive_mask)

    # --- Check 5: Unparseable listen_time ---
    invalid_ts_mask = F.col("listen_time").cast(TimestampType()).isNull()
    invalid_ts_rows = clean.filter(invalid_ts_mask).withColumn(
        "_quarantine_reason",
        F.concat(
            F.lit("invalid listen_time: '"),
            F.col("listen_time"),
            F.lit("' cannot be parsed as a timestamp"),
        ),
    )
    if invalid_ts_rows.count() > 0:
        quarantine_parts.append(invalid_ts_rows)
    clean = clean.filter(~invalid_ts_mask)

    # --- Check 6: Duplicate rows ---
    # Assign a stable per-row ID so row_number() has a deterministic tie-break
    # within each duplicate group.
    clean_with_id = clean.withColumn("_tmp_row_id", F.monotonically_increasing_id())
    window_spec = Window.partitionBy("user_id", "track_id", "listen_time").orderBy(
        "_tmp_row_id"
    )
    ranked = clean_with_id.withColumn("_dup_rank", F.row_number().over(window_spec))

    dup_rows = (
        ranked.filter(F.col("_dup_rank") > 1)
        .drop("_tmp_row_id", "_dup_rank")
        .withColumn(
            "_quarantine_reason",
            F.lit(
                "duplicate row: identical user_id, track_id, and listen_time "
                "as another row in this batch"
            ),
        )
    )
    if dup_rows.count() > 0:
        quarantine_parts.append(dup_rows)
    clean = ranked.filter(F.col("_dup_rank") == 1).drop("_tmp_row_id", "_dup_rank")

    quarantine_df = reduce(SparkDataFrame.union, quarantine_parts) if quarantine_parts else None
    return clean, quarantine_df


def write_quarantine(quarantine_df, quarantine_bucket):
    """Append *quarantine_df* to s3://<quarantine_bucket>/quarantine/ as Parquet.

    Uses ``mode("append")`` so successive pipeline runs accumulate quarantine
    records rather than overwriting them.

    Returns the quarantine S3 path written.
    """
    path = "s3://%s/quarantine/" % quarantine_bucket
    quarantine_df.write.mode("append").parquet(path)
    return path


def quarantine_invalid_rows(streams_df, quarantine_bucket, logger=None):
    """Validate stream rows, quarantine failures to S3, and return clean data.

    Orchestrates ``tag_invalid_rows`` + ``write_quarantine`` and emits
    per-reason counts to the logger so operators can see why rows were
    rejected without inspecting S3 manually.

    Returns:
        (clean_df, total_quarantined_count)

    Raises:
        RuntimeError: if every row is quarantined (no clean data to process).
    """
    def log(msg):
        if logger is not None:
            logger.info(msg)

    log("Starting data quality validation before KPI calculations.")
    clean, quarantine_df = tag_invalid_rows(streams_df)

    total_in = streams_df.count()
    total_clean = clean.count()
    total_quarantined = total_in - total_clean

    if quarantine_df is not None and total_quarantined > 0:
        # Log a per-reason breakdown so the CloudWatch log tells the full story.
        reason_counts = (
            quarantine_df.groupBy("_quarantine_reason")
            .count()
            .orderBy(F.col("count").desc())
            .collect()
        )
        for row in reason_counts:
            log(
                "QUARANTINE — reason: '%s' | affected rows: %d"
                % (row["_quarantine_reason"], row["count"])
            )

        quarantine_path = write_quarantine(quarantine_df, quarantine_bucket)
        log(
            "Wrote %d quarantined row(s) to %s."
            % (total_quarantined, quarantine_path)
        )
    else:
        log("No rows quarantined. All %d row(s) passed data quality checks." % total_in)

    log(
        "Data quality complete: %d valid row(s) of %d total will proceed to KPI calculation."
        % (total_clean, total_in)
    )

    if total_clean == 0:
        raise RuntimeError(
            "All %d stream row(s) were quarantined — no valid data remains "
            "for KPI computation. Check %s for details."
            % (total_in, "s3://%s/quarantine/" % quarantine_bucket)
        )

    return clean, total_quarantined


# ---------------------------------------------------------------------------
# Type casts
# ---------------------------------------------------------------------------

def cast_stream_types(streams_df):
    """Cast stream columns and drop rows whose casts produced nulls.

    ``user_id`` -> IntegerType, ``listen_time`` -> TimestampType.
    After ``quarantine_invalid_rows`` has already removed type-invalid rows,
    this should drop zero additional rows. The safety check is kept so the
    function remains correct when called in isolation (e.g. in unit tests).

    Returns:
        (cast_df, dropped_count)
    """
    casted = (
        streams_df.withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("listen_time", F.col("listen_time").cast(TimestampType()))
    )
    before = casted.count()
    cleaned = casted.filter(
        F.col("user_id").isNotNull() & F.col("listen_time").isNotNull()
    )
    after = cleaned.count()
    return cleaned, before - after


def cast_song_types(songs_df):
    """Cast ``duration_ms`` on the songs DataFrame to LongType."""
    return songs_df.withColumn("duration_ms", F.col("duration_ms").cast(LongType()))


# ---------------------------------------------------------------------------
# Enrichment and KPI computation
# ---------------------------------------------------------------------------

def join_streams_songs(streams_df, songs_df):
    """Inner-join streams to songs on ``track_id``.

    Rows with a null ``track_id`` on either side are dropped before the join so
    they never leak into the KPIs. Adds track_genre, duration_ms, track_name and
    artists to each stream event.
    """
    streams_clean = streams_df.filter(F.col("track_id").isNotNull())
    songs_clean = songs_df.filter(F.col("track_id").isNotNull()).select(
        "track_id", "track_genre", "duration_ms", "track_name", "artists"
    )
    return streams_clean.join(songs_clean, on="track_id", how="inner")


def add_date_column(df):
    """Add a ``date`` column derived from ``listen_time`` (used for grouping)."""
    return df.withColumn("date", F.to_date(F.col("listen_time")))


def compute_genre_kpis(df):
    """Compute genre-level daily KPIs grouped by (date, track_genre)."""
    kpis = df.groupBy("date", "track_genre").agg(
        F.count(F.lit(1)).alias("listen_count"),
        F.countDistinct("user_id").alias("unique_listeners"),
        F.sum("duration_ms").alias("total_listening_time_ms"),
    )
    return kpis.withColumn(
        "avg_listening_time_per_user_ms",
        F.col("total_listening_time_ms") / F.col("unique_listeners"),
    )


def compute_top_3_songs(df):
    """Compute the top 3 songs per (date, track_genre).

    Returns a DataFrame with one row per (date, track_genre) and a
    ``top_3_songs`` array-of-structs column: [{rank, track_name, artists,
    play_count}].
    """
    plays = df.groupBy(
        "date", "track_genre", "track_id", "track_name", "artists"
    ).agg(F.count(F.lit(1)).alias("play_count"))

    window = Window.partitionBy("date", "track_genre").orderBy(
        F.col("play_count").desc()
    )
    ranked = plays.withColumn("rank", F.rank().over(window)).filter(
        F.col("rank") <= 3
    )

    return ranked.groupBy("date", "track_genre").agg(
        F.collect_list(
            F.struct("rank", "track_name", "artists", "play_count")
        ).alias("top_3_songs")
    )


def mark_top_5_genres(kpi_df):
    """Add a boolean ``is_top_5`` column flagging the top 5 genres per day."""
    window = Window.partitionBy("date").orderBy(F.col("listen_count").desc())
    return (
        kpi_df.withColumn("genre_rank", F.rank().over(window))
        .withColumn("is_top_5", F.col("genre_rank") <= 5)
        .drop("genre_rank")
    )


def join_top_songs(kpi_df, top_songs_df):
    """Left-join the top_3_songs column onto the KPI DataFrame."""
    return kpi_df.join(top_songs_df, on=["date", "track_genre"], how="left")


def build_kpis(streams_df, songs_df, quarantine_bucket=None, logger=None):
    """Run the full transformation pipeline on already-loaded DataFrames.

    When *quarantine_bucket* is provided, invalid rows are quarantined to
    s3://<quarantine_bucket>/quarantine/ before KPI calculation begins so that
    KPIs are computed only on validated, clean data.

    Returns the final KPI DataFrame ready to be written as Parquet.
    """
    def log(msg):
        if logger is not None:
            logger.info(msg)

    if quarantine_bucket:
        streams_df, quarantined_count = quarantine_invalid_rows(
            streams_df, quarantine_bucket, logger=logger
        )
        log(
            "Quarantine step complete: %d row(s) rejected before KPI calculation."
            % quarantined_count
        )
    else:
        log(
            "No quarantine_bucket provided — skipping row-level data quality checks. "
            "Invalid rows will be silently dropped during type casting."
        )

    streams_cast, dropped = cast_stream_types(streams_df)
    if dropped:
        log(
            "cast_stream_types dropped %d row(s) with un-castable "
            "user_id/listen_time (these should have been caught by quarantine)." % dropped
        )

    songs_cast = cast_song_types(songs_df)

    before = streams_cast.count()
    enriched = join_streams_songs(streams_cast, songs_cast)
    after = enriched.count()
    log("Join streams->songs: %d rows before, %d rows after." % (before, after))

    dated = add_date_column(enriched)

    genre_kpis = compute_genre_kpis(dated)
    log("Computed genre KPIs: %d (date, genre) group(s)." % genre_kpis.count())

    top_songs = compute_top_3_songs(dated)
    flagged = mark_top_5_genres(genre_kpis)
    final = join_top_songs(flagged, top_songs)
    log("Final KPI dataframe: %d row(s)." % final.count())
    return final


def write_kpis(df, processed_bucket):
    """Write the KPI DataFrame as Parquet partitioned by ``date``.

    Output path: ``s3://{processed_bucket}/kpis/`` (partitionBy creates the
    ``date=YYYY-MM-DD/`` sub-paths). Mode is overwrite so re-running for the
    same date replaces the previous output.
    """
    output_path = "s3://%s/kpis/" % processed_bucket
    df.write.mode("overwrite").partitionBy("date").parquet(output_path)
    return output_path


def main():
    """Glue ETL entry point — wires GlueContext to the pure functions above."""
    import time

    from awsglue.context import GlueContext  # type: ignore
    from awsglue.job import Job  # type: ignore
    from awsglue.utils import getResolvedOptions  # type: ignore
    from pyspark.context import SparkContext  # type: ignore

    args = getResolvedOptions(
        __import__("sys").argv,
        ["JOB_NAME", "raw_bucket", "reference_bucket", "processed_bucket", "quarantine_bucket"],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger = glue_context.get_logger()
    start = time.time()
    logger.info("Transformation job '%s' started." % args["JOB_NAME"])

    raw_bucket = args["raw_bucket"]
    reference_bucket = args["reference_bucket"]
    processed_bucket = args["processed_bucket"]
    quarantine_bucket = args["quarantine_bucket"]

    streams_df = spark.read.csv("s3://%s/" % raw_bucket, header=True)
    if streams_df.rdd.isEmpty():
        raise RuntimeError(
            "No stream data found in s3://%s/ — aborting transformation."
            % raw_bucket
        )

    songs_df = spark.read.csv(
        "s3://%s/songs.csv" % reference_bucket, header=True
    )
    logger.info(
        "Loaded streams=%d, songs=%d rows."
        % (streams_df.count(), songs_df.count())
    )

    final = build_kpis(
        streams_df, songs_df, quarantine_bucket=quarantine_bucket, logger=logger
    )

    if final.rdd.isEmpty():
        logger.warn(
            "KPI dataframe is empty — writing an empty Parquet output so "
            "downstream jobs do not fail on a missing path."
        )

    output_path = write_kpis(final, processed_bucket)
    elapsed = time.time() - start
    logger.info(
        "Transformation job complete. Output: %s. Elapsed: %.1fs."
        % (output_path, elapsed)
    )
    job.commit()


if __name__ == "__main__":
    main()
