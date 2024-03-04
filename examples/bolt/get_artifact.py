# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

BOLT_SCHEDULE_RUN_ID = 1  # Replace with the run ID of the Bolt schedule to get the manifest for


commands = paradime.bolt.list_run_commands(BOLT_SCHEDULE_RUN_ID)  # Get the commands for the run

for command in commands:
    # Get the artifacts for the command
    artifacts = paradime.bolt.list_command_artifacts(command.id)

    for artifact in artifacts:
        # If the artifact is the manifest, get the URL and print it
        if "manifest.json" in artifact.path:
            manifest_url = paradime.bolt.get_artifact_url(artifact.id)
            print(f"Manifest URL: {manifest_url}")
            break
