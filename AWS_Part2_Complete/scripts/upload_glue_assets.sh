#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/upload_glue_assets.sh <artifact-bucket>
#
# Example:
#   ./scripts/upload_glue_assets.sh my-trade-artifacts-bucket

BUCKET="${1:?artifact bucket is required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

aws s3 cp \
  "${ROOT}/ingestion/glue/trade_invoice_etl.py" \
  "s3://${BUCKET}/artifacts/glue/trade_invoice_etl.py"

aws s3 cp \
  "${ROOT}/ingestion/glue/sql/" \
  "s3://${BUCKET}/artifacts/glue/sql/" \
  --recursive \
  --exclude "*" \
  --include "*.sql"

echo "GlueScriptS3Uri=s3://${BUCKET}/artifacts/glue/trade_invoice_etl.py"
echo "GlueSqlBaseUri=s3://${BUCKET}/artifacts/glue/sql"
