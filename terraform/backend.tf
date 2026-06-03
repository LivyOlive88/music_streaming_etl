# ---------------------------------------------------------------------------
# backend.tf
# Configures local state storage. State is kept on the local filesystem; the
# state files are gitignored and must never be committed (they can contain
# sensitive resource details).
# ---------------------------------------------------------------------------

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}
