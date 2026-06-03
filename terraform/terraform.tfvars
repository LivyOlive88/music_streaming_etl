# ---------------------------------------------------------------------------
# terraform.tfvars
# Non-secret variable values for this deployment. No credentials live here.
# ---------------------------------------------------------------------------

aws_region          = "eu-west-1"
project_name        = "music-streaming-etl"
environment         = "dev"
bucket_prefix       = "music-streaming-etl"
dynamodb_table_name = "music_streaming_kpis"
glue_database_name  = "music_streaming_db"

tags = {
  Team = "data-engineering"
}
