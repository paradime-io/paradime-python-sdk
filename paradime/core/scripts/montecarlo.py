import logging
import os
import subprocess
from pathlib import Path
from typing import Final, Optional, Tuple

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

PARADIME_RESOURCES_DIRECTORY_ENV_VAR: Final = "PARADIME_RESOURCE_DIRECTORY"
PARADIME_SCHEDULE_NAME_ENV_VAR: Final = "PARADIME_SCHEDULE_NAME"
MONTECARLO_PROJECT_NAME_ENV_VAR: Final = "MONTECARLO_PROJECT_NAME"
MONTECARLO_CONNECTION_ID_ENV_VAR: Final = "MONTECARLO_CONNECTION_ID"


def search_for_files_to_upload_to_montecarlo(
    *,
    paradime_resources_directory: str,
    paradime_schedule_name: str,
    project_name: str,
    connection_id: str,
) -> Tuple[bool, bool]:
    success, found_files = True, False
    for root, dirs, files in os.walk(paradime_resources_directory):
        # Check for the presence of directories with both the manifest.json and run_results.json in a target folder
        manifest_path = Path(root) / "target" / "manifest.json"
        run_results_path = Path(root) / "target" / "run_results.json"
        logs_path = Path(root) / "logs" / "dbt.log"

        if manifest_path.is_file() and run_results_path.is_file():
            found_files = True

            # Run the CLI command
            try:
                _run_montecarlo_import(
                    manifest_path=manifest_path,
                    run_results_path=run_results_path,
                    project_name=project_name,
                    job_name=paradime_schedule_name,
                    connection_id=connection_id,
                    logs_path=logs_path if logs_path.is_file() else None,
                )
            except Exception as e:
                logger.error(f"Error running montecarlo import: {e!r}")
                success = False

    return success, found_files


def _run_montecarlo_import(
    *,
    manifest_path: Path,
    run_results_path: Path,
    project_name: str,
    job_name: str,
    connection_id: str,
    logs_path: Optional[Path],
) -> None:
    command = [
        "montecarlo",
        "import",
        "dbt-run",
        "--project-name",
        project_name,
        "--job-name",
        job_name,
        "--manifest",
        str(manifest_path),
        "--run-results",
        str(run_results_path),
        "--connection-id",
        connection_id,
    ]

    if logs_path:
        command += ["--logs", str(logs_path)]

    try:
        logger.info(f"Running montecarlo import command: {command!r}")
        result = subprocess.run(command, check=True, capture_output=True, text=True, env=os.environ)
        logger.info(f"Montecarlo import command result: {result.stdout} {result.stderr}")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error running montecarlo import: {e.stdout!r} {e.stderr!r}")
