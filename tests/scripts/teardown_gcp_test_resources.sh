#!/bin/bash
set -euo pipefail

# =============================================================================
# GCP Test Resources Teardown Script for Paradime SDK
# =============================================================================
# Cleans up all test resources created by setup_gcp_test_resources.sh
#
# Usage:
#   ./scripts/teardown_gcp_test_resources.sh <PROJECT_ID> [REGION]
#
# Example:
#   ./scripts/teardown_gcp_test_resources.sh my-gcp-project us-central1
# =============================================================================

if [ $# -lt 1 ]; then
    echo "Usage: $0 <PROJECT_ID> [REGION]"
    exit 1
fi

PROJECT_ID="$1"
REGION="${2:-us-central1}"
SA_NAME="paradime-sdk-test"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUCKET_NAME="${PROJECT_ID}-paradime-test"

echo "============================================================"
echo "  Paradime SDK - GCP Test Resources Teardown"
echo "============================================================"
echo "  Project: ${PROJECT_ID}"
echo "  Region:  ${REGION}"
echo "============================================================"
echo ""
echo "  This will DELETE the following resources:"
echo "    - Dataproc cluster: paradime-test-cluster"
echo "    - Cloud Run Job: paradime-test-job"
echo "    - Cloud Function: paradime-test-function"
echo "    - Cloud SQL instance: paradime-test-mysql"
echo "    - GCS bucket: gs://${BUCKET_NAME}"
echo "    - BigQuery dataset: paradime_test"
echo "    - BigQuery scheduled queries named 'paradime-test-scheduled-query'"
echo "    - Service account: ${SA_EMAIL}"
echo "    - Local key file: ${REPO_ROOT}/paradime-sa-key.json"
echo "    - Local .env file: ${REPO_ROOT}/.env.gcp-test"
echo ""
read -p "  Are you sure? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "  Aborted."
    exit 0
fi
echo ""

# ---------------------------------------------------------
# Dataproc cluster
# ---------------------------------------------------------
echo ">>> Deleting Dataproc cluster..."
gcloud dataproc clusters delete paradime-test-cluster \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Cloud Run Job
# ---------------------------------------------------------
echo ">>> Deleting Cloud Run Job..."
gcloud run jobs delete paradime-test-job \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Cloud Function
# ---------------------------------------------------------
echo ">>> Deleting Cloud Function..."
gcloud functions delete paradime-test-function \
    --gen2 \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Cloud SQL instance (this deletes the database too)
# ---------------------------------------------------------
echo ">>> Deleting Cloud SQL instance (this may take a few minutes)..."
gcloud sql instances delete paradime-test-mysql \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Datastream streams (if any were created)
# ---------------------------------------------------------
echo ">>> Checking for Datastream streams..."
echo "    NOTE: Datastream streams must be stopped before deletion."
echo "    If you created a stream named 'paradime-test-stream', delete it manually:"
echo "    https://console.cloud.google.com/datastream/streams?project=${PROJECT_ID}"

# ---------------------------------------------------------
# BigQuery scheduled query transfers
# ---------------------------------------------------------
echo ">>> Deleting BigQuery scheduled queries..."
echo "    NOTE: Use the BigQuery Console to delete scheduled queries:"
echo "    https://console.cloud.google.com/bigquery/scheduled-queries?project=${PROJECT_ID}"

# ---------------------------------------------------------
# BigQuery dataset
# ---------------------------------------------------------
echo ">>> Deleting BigQuery dataset..."
bq rm -r -f "${PROJECT_ID}:paradime_test" 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# GCS bucket
# ---------------------------------------------------------
echo ">>> Deleting GCS bucket..."
gsutil -m rm -r "gs://${BUCKET_NAME}" 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Service account
# ---------------------------------------------------------
echo ">>> Deleting service account..."
gcloud iam service-accounts delete "${SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null \
    && echo "    ✅ Deleted." \
    || echo "    ⚠️ Not found or already deleted."

# ---------------------------------------------------------
# Local files
# ---------------------------------------------------------
echo ">>> Cleaning up local files..."
if [ -f "${REPO_ROOT}/paradime-sa-key.json" ]; then
    rm "${REPO_ROOT}/paradime-sa-key.json"
    echo "    ✅ Deleted ${REPO_ROOT}/paradime-sa-key.json"
else
    echo "    ⚠️ ${REPO_ROOT}/paradime-sa-key.json not found."
fi

if [ -f "${REPO_ROOT}/.env.gcp-test" ]; then
    rm "${REPO_ROOT}/.env.gcp-test"
    echo "    ✅ Deleted ${REPO_ROOT}/.env.gcp-test"
else
    echo "    ⚠️ ${REPO_ROOT}/.env.gcp-test not found."
fi

echo ""
echo "============================================================"
echo "  TEARDOWN COMPLETE"
echo "============================================================"
echo ""
echo "  Some resources may need manual cleanup:"
echo "    - Datastream streams/connection profiles"
echo "    - BigQuery scheduled query transfer configs"
echo "    - Any Dataflow jobs still running"
echo ""
echo "  Check the GCP Console for any remaining resources:"
echo "    https://console.cloud.google.com/home/dashboard?project=${PROJECT_ID}"
echo "============================================================"
