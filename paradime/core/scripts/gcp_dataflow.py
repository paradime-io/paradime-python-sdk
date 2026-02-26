import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional

from paradime.core.scripts.gcp_utils import get_gcp_credentials

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_templates_client(service_account_key_file: str) -> Any:
    from google.cloud import dataflow_v1beta3

    credentials = get_gcp_credentials(service_account_key_file)
    return dataflow_v1beta3.TemplatesServiceClient(credentials=credentials, transport="rest")


def _get_flex_templates_client(service_account_key_file: str) -> Any:
    from google.cloud import dataflow_v1beta3

    credentials = get_gcp_credentials(service_account_key_file)
    return dataflow_v1beta3.FlexTemplatesServiceClient(credentials=credentials, transport="rest")


def _get_jobs_client(service_account_key_file: str) -> Any:
    from google.cloud import dataflow_v1beta3

    credentials = get_gcp_credentials(service_account_key_file)
    return dataflow_v1beta3.JobsV1Beta3Client(credentials=credentials, transport="rest")


def trigger_dataflow_job(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    template_path: str,
    job_name: str,
    template_type: str = "classic",
    parameters: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """Launch a Dataflow job from a template."""

    print(f"\n{'='*60}")
    print("🚀 LAUNCHING DATAFLOW JOB")
    print(f"{'='*60}")
    print(f"   Template: {template_path}")
    print(f"   Type: {template_type}")
    print(f"   Job Name: {job_name}")
    print(f"{'-'*60}")

    result = _launch_dataflow_job(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
        template_path=template_path,
        job_name=job_name,
        template_type=template_type,
        parameters=parameters,
        wait_for_completion=wait_for_completion,
        timeout_minutes=timeout_minutes,
    )

    print(f"\n{'='*80}")
    print("📊 RESULT")
    print(f"{'='*80}")

    if "DONE" in result or "SUCCESS" in result:
        print(f"   ✅ {result}")
    elif "FAILED" in result:
        print(f"   ❌ {result}")
    else:
        print(f"   ℹ️ {result}")

    print(f"{'='*80}\n")

    return [result]


def _launch_dataflow_job(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    template_path: str,
    job_name: str,
    template_type: str,
    parameters: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> str:
    from google.cloud import dataflow_v1beta3

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    # Parse parameters
    params: Dict[str, str] = {}
    if parameters:
        try:
            params = json.loads(parameters)
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON parameters: {e}")

    job_id: str

    if template_type == "flex":
        print(f"{timestamp} 🚀 [{job_name}] Launching flex template...")
        client = _get_flex_templates_client(service_account_key_file)

        request = dataflow_v1beta3.LaunchFlexTemplateRequest(
            project_id=project,
            location=location,
            launch_parameter=dataflow_v1beta3.LaunchFlexTemplateParameter(
                job_name=job_name,
                container_spec_gcs_path=template_path,
                parameters=params,
            ),
        )
        response = client.launch_flex_template(request=request)
        job_id = response.job.id
    else:
        print(f"{timestamp} 🚀 [{job_name}] Launching classic template...")
        client = _get_templates_client(service_account_key_file)

        request = dataflow_v1beta3.LaunchTemplateRequest(
            project_id=project,
            location=location,
            gcs_path=template_path,
            launch_parameters=dataflow_v1beta3.LaunchTemplateParameters(
                job_name=job_name,
                parameters=params,
            ),
        )
        response = client.launch_template(request=request)
        job_id = response.job.id

    print(f"{timestamp} ✅ [{job_name}] Job launched with ID: {job_id}")
    console_url = (
        f"https://console.cloud.google.com/dataflow/jobs/{location}/{job_id}" f"?project={project}"
    )
    print(f"{timestamp} 🔗 [{job_name}] Console: {console_url}")

    if not wait_for_completion:
        return f"TRIGGERED (job_id: {job_id})"

    print(f"{timestamp} ⏳ [{job_name}] Monitoring job progress...")
    return _wait_for_dataflow_job(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
        job_id=job_id,
        job_name=job_name,
        timeout_minutes=timeout_minutes,
    )


def _wait_for_dataflow_job(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    job_id: str,
    job_name: str,
    timeout_minutes: int,
) -> str:
    from google.cloud import dataflow_v1beta3

    jobs_client = _get_jobs_client(service_account_key_file)
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10
    counter = 0

    terminal_states = {
        dataflow_v1beta3.JobState.JOB_STATE_DONE,
        dataflow_v1beta3.JobState.JOB_STATE_FAILED,
        dataflow_v1beta3.JobState.JOB_STATE_CANCELLED,
        dataflow_v1beta3.JobState.JOB_STATE_DRAINED,
        dataflow_v1beta3.JobState.JOB_STATE_UPDATED,
    }

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for Dataflow job '{job_name}' after {timeout_minutes} minutes"
            )

        try:
            job = jobs_client.get_job(
                request=dataflow_v1beta3.GetJobRequest(
                    project_id=project,
                    job_id=job_id,
                    location=location,
                )
            )
            state = job.current_state

            if counter == 0 or counter % 6 == 0:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                state_name = dataflow_v1beta3.JobState(state).name
                print(
                    f"{timestamp} 🔄 [{job_name}] State: {state_name} "
                    f"({elapsed_min}m {elapsed_sec}s elapsed)"
                )

            if state in terminal_states:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                state_name = dataflow_v1beta3.JobState(state).name

                if state == dataflow_v1beta3.JobState.JOB_STATE_DONE:
                    print(
                        f"{timestamp} ✅ [{job_name}] Completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"DONE ({elapsed_min}m {elapsed_sec}s)"
                elif state == dataflow_v1beta3.JobState.JOB_STATE_FAILED:
                    print(f"{timestamp} ❌ [{job_name}] Job failed")
                    return "FAILED"
                else:
                    print(f"{timestamp} ⚠️ [{job_name}] Job ended with state: {state_name}")
                    return f"{state_name}"

        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️ [{job_name}] Error checking status: {str(e)[:80]}...")

        counter += 1
        time.sleep(sleep_interval)
