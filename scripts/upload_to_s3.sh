#!/usr/bin/env bash
# Uploads local Gold layer parquet files to S3

set -euo pipefail

GOLD_BUCKET=$(cd terraform && terraform output -raw gold_bucket)
LOCAL_GOLD="data/gold"

echo "Uploading Gold layer to s3://${GOLD_BUCKET}/"
aws s3 sync "${LOCAL_GOLD}/" "s3://${GOLD_BUCKET}/gold/" \
  --exclude "*.DS_Store" \
  --storage-class INTELLIGENT_TIERING

echo "Upload complete."
aws s3 ls "s3://${GOLD_BUCKET}/gold/" --human-readable
