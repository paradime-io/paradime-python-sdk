#!/bin/bash
set -euo pipefail

# =============================================================================
# GCP Test Resources Setup Script for Paradime SDK
# =============================================================================
# This script creates all necessary GCP resources to test the 6 GCP integration
# commands in the Paradime SDK:
#   1. BigQuery Scheduled Query
#   2. Cloud Function
#   3. Cloud Run Job
#   4. Dataflow (uses Google-provided template)
#   5. Dataproc Cluster + PySpark script
#   6. Datastream (Cloud SQL MySQL + BigQuery)
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Billing enabled on the target project
#   - User has Owner or Editor role on the project
#
# Usage:
#   ./scripts/setup_gcp_test_resources.sh <PROJECT_ID> [REGION]
#
# Example:
#   ./scripts/setup_gcp_test_resources.sh my-gcp-project us-central1
# =============================================================================

if [ $# -lt 1 ]; then
    echo "Usage: $0 <PROJECT_ID> [REGION]"
    echo ""
    echo "  PROJECT_ID  Your GCP project ID"
    echo "  REGION      GCP region (default: us-central1)"
    exit 1
fi

PROJECT_ID="$1"
REGION="${2:-us-central1}"
SA_NAME="paradime-sdk-test"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE="$(pwd)/paradime-sa-key.json"
BUCKET_NAME="${PROJECT_ID}-paradime-test"
BQ_LOCATION="us"  # BigQuery Data Transfer uses multi-region locations

echo "============================================================"
echo "  Paradime SDK - GCP Test Resources Setup"
echo "============================================================"
echo "  Project:  ${PROJECT_ID}"
echo "  Region:   ${REGION}"
echo "  SA:       ${SA_EMAIL}"
echo "  Key File: ${KEY_FILE}"
echo "  Bucket:   gs://${BUCKET_NAME}"
echo "============================================================"
echo ""

# ---------------------------------------------------------
# Step 1: Enable required APIs
# ---------------------------------------------------------
echo ">>> Step 1: Enabling required APIs..."
APIS=(
    "bigquerydatatransfer.googleapis.com"
    "cloudfunctions.googleapis.com"
    "cloudbuild.googleapis.com"
    "run.googleapis.com"
    "dataflow.googleapis.com"
    "dataproc.googleapis.com"
    "datastream.googleapis.com"
    "sqladmin.googleapis.com"
    "compute.googleapis.com"
    "artifactregistry.googleapis.com"
)

for api in "${APIS[@]}"; do
    echo "    Enabling ${api}..."
    gcloud services enable "${api}" --project="${PROJECT_ID}" --quiet
done
echo "    ✅ All APIs enabled."
echo ""

# ---------------------------------------------------------
# Step 2: Create service account and download key
# ---------------------------------------------------------
echo ">>> Step 2: Creating service account..."
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    Service account already exists: ${SA_EMAIL}"
else
    gcloud iam service-accounts create "${SA_NAME}" \
        --display-name="Paradime SDK Test" \
        --project="${PROJECT_ID}"
    echo "    ✅ Service account created: ${SA_EMAIL}"
fi

echo "    Granting roles..."
ROLES=(
    "roles/bigquery.admin"
    "roles/bigquery.user"
    "roles/cloudfunctions.invoker"
    "roles/cloudfunctions.viewer"
    "roles/run.invoker"
    "roles/run.admin"
    "roles/dataflow.admin"
    "roles/dataflow.worker"
    "roles/dataproc.editor"
    "roles/datastream.admin"
    "roles/storage.admin"
    "roles/iam.serviceAccountUser"
    "roles/compute.viewer"
)

for role in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${role}" \
        --quiet --no-user-output-enabled
done
echo "    ✅ All roles granted."

echo "    Downloading service account key..."
if [ -f "${KEY_FILE}" ]; then
    echo "    Key file already exists at ${KEY_FILE}, skipping download."
else
    gcloud iam service-accounts keys create "${KEY_FILE}" \
        --iam-account="${SA_EMAIL}"
    echo "    ✅ Key saved to ${KEY_FILE}"
fi
echo ""

# ---------------------------------------------------------
# Step 3: Create GCS bucket for test artifacts
# ---------------------------------------------------------
echo ">>> Step 3: Creating GCS bucket..."
if gsutil ls "gs://${BUCKET_NAME}" &>/dev/null; then
    echo "    Bucket already exists: gs://${BUCKET_NAME}"
else
    gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET_NAME}"
    echo "    ✅ Bucket created: gs://${BUCKET_NAME}"
fi
echo ""

# ---------------------------------------------------------
# Step 4: BigQuery Scheduled Query
# ---------------------------------------------------------
echo ">>> Step 4: Setting up BigQuery Scheduled Query..."
# Create dataset
if bq show --project_id="${PROJECT_ID}" "paradime_test" &>/dev/null; then
    echo "    Dataset paradime_test already exists."
else
    bq mk --dataset --location="${BQ_LOCATION}" "${PROJECT_ID}:paradime_test"
    echo "    ✅ Dataset paradime_test created."
fi

# Create a scheduled query using the BigQuery Data Transfer API via bq CLI
echo "    Creating scheduled query 'paradime-test-scheduled-query'..."
echo "    NOTE: Scheduled queries are best created via the BigQuery Console."
echo "    Please create one manually if the bq transfer command below fails:"
echo ""
echo "      1. Go to: https://console.cloud.google.com/bigquery/scheduled-queries?project=${PROJECT_ID}"
echo "      2. Click 'Create scheduled query'"
echo "      3. Query: SELECT CURRENT_TIMESTAMP() as run_time, 'paradime_test' as status"
echo "      4. Set schedule to 'On demand' or 'Every 24 hours'"
echo "      5. Destination table: ${PROJECT_ID}.paradime_test.scheduled_query_results"
echo "      6. Display name: paradime-test-scheduled-query"
echo ""

# Try creating via bq CLI (may require interactive setup for the first time)
bq mk --transfer_config \
    --project_id="${PROJECT_ID}" \
    --location="${BQ_LOCATION}" \
    --display_name="paradime-test-scheduled-query" \
    --data_source="scheduled_query" \
    --target_dataset="paradime_test" \
    --schedule="every 24 hours" \
    --params='{"query":"SELECT CURRENT_TIMESTAMP() as run_time, '\''paradime_test'\'' as status","destination_table_name_template":"scheduled_query_results","write_disposition":"WRITE_APPEND"}' \
    2>/dev/null || echo "    ⚠️ Scheduled query creation via CLI may require manual setup (see instructions above)."
echo ""

# ---------------------------------------------------------
# Step 5: Cloud Function
# ---------------------------------------------------------
echo ">>> Step 5: Deploying Cloud Function..."
FUNC_DIR=$(mktemp -d)
cat > "${FUNC_DIR}/main.py" << 'PYEOF'
import functions_framework
import json
import datetime

@functions_framework.http
def hello_http(request):
    """Simple test function for Paradime SDK testing."""
    request_json = request.get_json(silent=True)
    return json.dumps({
        "status": "ok",
        "message": "Hello from Paradime test function!",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "input": request_json,
    })
PYEOF

cat > "${FUNC_DIR}/requirements.txt" << 'REQEOF'
functions-framework==3.*
REQEOF

gcloud functions deploy paradime-test-function \
    --gen2 \
    --runtime=python312 \
    --region="${REGION}" \
    --source="${FUNC_DIR}" \
    --entry-point=hello_http \
    --trigger-http \
    --no-allow-unauthenticated \
    --project="${PROJECT_ID}" \
    --quiet \
    || echo "    ⚠️ Cloud Function deployment failed. You may need to run this manually."

rm -rf "${FUNC_DIR}"
echo "    ✅ Cloud Function deployed."
echo ""

# ---------------------------------------------------------
# Step 6: Cloud Run Job
# ---------------------------------------------------------
echo ">>> Step 6: Creating Cloud Run Job..."
if gcloud run jobs describe paradime-test-job --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    Cloud Run Job already exists."
else
    gcloud run jobs create paradime-test-job \
        --image=ubuntu \
        --command="echo" \
        --args="Paradime Cloud Run Job test completed successfully" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet
    echo "    ✅ Cloud Run Job created."
fi
echo ""

# ---------------------------------------------------------
# Step 7: Dataflow test input
# ---------------------------------------------------------
echo ">>> Step 7: Setting up Dataflow test data..."
echo "hello world hello paradime test dataflow pipeline words" | \
    gsutil cp - "gs://${BUCKET_NAME}/dataflow-test/input/test.txt"
echo "    ✅ Test input file uploaded to gs://${BUCKET_NAME}/dataflow-test/input/test.txt"
echo ""
echo "    To trigger a Dataflow job, use the Google-provided Word Count template:"
echo "      --template-path gs://dataflow-templates-${REGION}/latest/Word_Count"
echo "      --parameters '{\"inputFile\": \"gs://${BUCKET_NAME}/dataflow-test/input/test.txt\", \"output\": \"gs://${BUCKET_NAME}/dataflow-test/output/wordcount\"}'"
echo ""

# ---------------------------------------------------------
# Step 8: Dataproc cluster + PySpark script
# ---------------------------------------------------------
echo ">>> Step 8: Setting up Dataproc..."
# Upload test PySpark script
cat > /tmp/paradime_test_spark.py << 'SPARKEOF'
from pyspark.sql import SparkSession
import datetime

spark = SparkSession.builder.appName("ParadimeSDKTest").getOrCreate()

data = [
    ("Paradime", 1, str(datetime.datetime.now())),
    ("SDK", 2, str(datetime.datetime.now())),
    ("Test", 3, str(datetime.datetime.now())),
    ("Success", 4, str(datetime.datetime.now())),
]
df = spark.createDataFrame(data, ["word", "count", "timestamp"])
df.show()

print("=" * 60)
print("Paradime SDK Dataproc test completed successfully!")
print("=" * 60)

spark.stop()
SPARKEOF

gsutil cp /tmp/paradime_test_spark.py "gs://${BUCKET_NAME}/scripts/paradime_test_spark.py"
rm /tmp/paradime_test_spark.py
echo "    ✅ PySpark script uploaded."

# Grant storage permissions to the default Compute Engine service account
# (required for Dataproc VM service account to access GCS staging buckets)
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
echo "    Granting storage permissions to default compute SA: ${COMPUTE_SA}..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${COMPUTE_SA}" \
    --role="roles/storage.admin" \
    --quiet --no-user-output-enabled 2>/dev/null || true

# Create single-node cluster
if gcloud dataproc clusters describe paradime-test-cluster --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    Dataproc cluster already exists."
else
    echo "    Creating single-node Dataproc cluster (this may take 2-3 minutes)..."
    gcloud dataproc clusters create paradime-test-cluster \
        --region="${REGION}" \
        --single-node \
        --master-machine-type=e2-standard-2 \
        --master-boot-disk-size=50GB \
        --project="${PROJECT_ID}" \
        --quiet \
        || echo "    ⚠️ Cluster creation failed. You may need to check quotas or run manually."
    echo "    ✅ Dataproc cluster created."
fi
echo ""

# ---------------------------------------------------------
# Step 9: Datastream (MySQL -> BigQuery)
# ---------------------------------------------------------
echo ">>> Step 9: Setting up Datastream prerequisites..."
echo "    NOTE: Datastream requires a full source database setup."
echo "    This script will create the Cloud SQL instance, but the stream"
echo "    and connection profiles should be created via the Console for"
echo "    initial setup."
echo ""

# Create Cloud SQL MySQL instance
if gcloud sql instances describe paradime-test-mysql --project="${PROJECT_ID}" &>/dev/null; then
    echo "    Cloud SQL instance already exists."
else
    echo "    Creating Cloud SQL MySQL instance (this may take 5-10 minutes)..."
    gcloud sql instances create paradime-test-mysql \
        --database-version=MYSQL_8_0 \
        --tier=db-f1-micro \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet \
        || echo "    ⚠️ Cloud SQL creation failed."
fi

# Create test database
gcloud sql databases create paradime_test_db \
    --instance=paradime-test-mysql \
    --project="${PROJECT_ID}" \
    2>/dev/null || echo "    Database paradime_test_db may already exist."

echo ""
echo "    To complete Datastream setup, follow these steps in the Console:"
echo "      1. Go to: https://console.cloud.google.com/datastream/streams?project=${PROJECT_ID}"
echo "      2. Create a source connection profile for the MySQL instance"
echo "      3. Create a destination connection profile for BigQuery"
echo "      4. Create a stream with display name: paradime-test-stream"
echo ""

# ---------------------------------------------------------
# Step 10: Generate .env file
# ---------------------------------------------------------
echo ">>> Step 10: Generating .env file..."
ENV_FILE="$(pwd)/.env.gcp-test"
cat > "${ENV_FILE}" << ENVEOF
# GCP Test Environment Variables for Paradime SDK
# Generated on $(date -u +"%Y-%m-%dT%H:%M:%SZ")

GCP_SERVICE_ACCOUNT_KEY_FILE=${KEY_FILE}
GCP_PROJECT_ID=${PROJECT_ID}
GCP_LOCATION=${REGION}
ENVEOF

echo "    ✅ .env file saved to ${ENV_FILE}"
echo ""

# ---------------------------------------------------------
# Summary
# ---------------------------------------------------------
echo "============================================================"
echo "  SETUP COMPLETE"
echo "============================================================"
echo ""
echo "  Environment file: ${ENV_FILE}"
echo "  Service account key: ${KEY_FILE}"
echo ""
echo "  Test commands:"
echo ""
echo "  # Load env vars"
echo "  export \$(cat ${ENV_FILE} | grep -v '^#' | xargs)"
echo ""
echo "  # BigQuery Scheduled Query"
echo "  paradime run gcp-bigquery-transfer-list"
echo "  paradime run gcp-bigquery-transfer-trigger \\"
echo "    --scheduled-query-name 'paradime-test-scheduled-query' \\"
echo "    --location ${BQ_LOCATION} --wait-for-completion"
echo ""
echo "  # Cloud Function"
echo "  paradime run gcp-cloud-function-list"
echo "  paradime run gcp-cloud-function-trigger \\"
echo "    --function-name paradime-test-function --wait-for-completion"
echo ""
echo "  # Cloud Run Job"
echo "  paradime run gcp-cloud-run-list"
echo "  paradime run gcp-cloud-run-trigger \\"
echo "    --job-name paradime-test-job --wait-for-completion"
echo ""
echo "  # Dataflow"
echo "  paradime run gcp-dataflow-list"
echo "  paradime run gcp-dataflow-trigger \\"
echo "    --template-path gs://dataflow-templates-${REGION}/latest/Word_Count \\"
echo "    --job-name paradime-test-wordcount \\"
echo "    --parameters '{\"inputFile\": \"gs://${BUCKET_NAME}/dataflow-test/input/test.txt\", \"output\": \"gs://${BUCKET_NAME}/dataflow-test/output/wordcount\"}' \\"
echo "    --wait-for-completion"
echo ""
echo "  # Dataproc"
echo "  paradime run gcp-dataproc-list-clusters"
echo "  paradime run gcp-dataproc-trigger \\"
echo "    --cluster-name paradime-test-cluster \\"
echo "    --job-type pyspark \\"
echo "    --main-file gs://${BUCKET_NAME}/scripts/paradime_test_spark.py \\"
echo "    --wait-for-completion"
echo ""
echo "  # Datastream"
echo "  paradime run gcp-datastream-list"
echo "  paradime run gcp-datastream-trigger \\"
echo "    --stream-name paradime-test-stream \\"
echo "    --action start --wait-for-completion"
echo ""
echo "============================================================"
echo "  COST WARNING: Resources like Dataproc cluster and Cloud SQL"
echo "  incur costs while running. Run the teardown script when done:"
echo "    ./scripts/teardown_gcp_test_resources.sh ${PROJECT_ID} ${REGION}"
echo "============================================================"
