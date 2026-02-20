# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

BOLT_SCHEDULE_NAME = "daily_run"

# Get all artifacts (including manifest.json).
artifacts = paradime.bolt.get_all_latest_artifacts(schedule_name=BOLT_SCHEDULE_NAME)
manifest_json = artifacts.get("manifest")


# Get any artifact.
artifact_url = paradime.bolt.get_latest_artifact_url(
    schedule_name=BOLT_SCHEDULE_NAME, artifact_path="target/catalog.json"
)
