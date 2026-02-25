import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

from paradime.core.scripts.gcp_utils import get_gcp_credentials

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_jobs_client(service_account_key_file: str) -> Any:
    from google.cloud import run_v2

    credentials = get_gcp_credentials(service_account_key_file)
    return run_v2.JobsClient(credentials=credentials, transport="rest")


def _get_executions_client(service_account_key_file: str) -> Any:
    from google.cloud import run_v2

    credentials = get_gcp_credentials(service_account_key_file)
    return run_v2.ExecutionsClient(credentials=credentials, transport="rest")


def trigger_cloud_run_jobs(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    job_names: List[str],
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """Trigger Cloud Run Jobs by name."""
    futures = []
    results = []

    print(f"\n{'='*60}")
    print("🚀 TRIGGERING CLOUD RUN JOBS")
    print(f"{'='*60}")

    with ThreadPoolExecutor() as executor:
        for i, name in enumerate(set(job_names), 1):
            print(f"\n[{i}/{len(set(job_names))}] 🐳 {name}")
            print(f"{'-'*40}")

            futures.append(
                (
                    name,
                    executor.submit(
                        _trigger_single_job,
                        service_account_key_file=service_account_key_file,
                        project=project,
                        location=location,
                        job_name=name,
                        wait_for_completion=wait_for_completion,
                        timeout_minutes=timeout_minutes,
                    ),
                )
            )

        print(f"\n{'='*60}")
        print("⚡ LIVE PROGRESS")
        print(f"{'='*60}")

        job_results = []
        for name, future in futures:
            future_timeout = (timeout_minutes * 60 + 120) if wait_for_completion else 120
            response_txt = future.result(timeout=future_timeout)
            job_results.append((name, response_txt))
            results.append(response_txt)

        print(f"\n{'='*80}")
        print("📊 RESULTS")
        print(f"{'='*80}")
        print(f"{'JOB NAME':<40} {'STATUS'}")
        print(f"{'-'*40} {'-'*30}")

        for name, response_txt in job_results:
            if "SUCCEEDED" in response_txt or "SUCCESS" in response_txt:
                status = "✅ SUCCEEDED"
            elif "FAILED" in response_txt:
                status = "❌ FAILED"
            else:
                status = "ℹ️ TRIGGERED"
            print(f"{name:<40} {status}")

        print(f"{'='*80}\n")

    return results


def _trigger_single_job(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    job_name: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> str:

    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    full_name = f"projects/{project}/locations/{location}/jobs/{job_name}"

    jobs_client = _get_jobs_client(service_account_key_file)

    print(f"{timestamp} 🚀 [{job_name}] Triggering job execution...")
    # run_job returns an LRO, but we don't use operation.result() because
    # REST transport has metadata parsing issues. Instead we poll the job's
    # latest execution directly.
    jobs_client.run_job(name=full_name)

    if not wait_for_completion:
        print(f"{timestamp} ✅ [{job_name}] Job triggered (not waiting for completion)")
        return "TRIGGERED"

    print(f"{timestamp} ⏳ [{job_name}] Waiting for job to complete...")

    return _wait_for_job_completion(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
        job_name=job_name,
        timeout_minutes=timeout_minutes,
    )


def _poll_execution_rest(
    service_account_key_file: str,
    execution_name: str,
) -> dict:
    """Poll execution status using the REST API directly."""
    import google.auth.transport.requests
    import requests as http_requests

    credentials = get_gcp_credentials(service_account_key_file)
    auth_request = google.auth.transport.requests.Request()
    credentials.refresh(auth_request)

    # execution_name format: projects/{project}/locations/{location}/jobs/{job}/executions/{exec}
    url = f"https://run.googleapis.com/v2/{execution_name}"
    response = http_requests.get(
        url,
        headers={"Authorization": f"Bearer {credentials.token}"},
    )
    response.raise_for_status()
    return response.json()


def _wait_for_job_completion(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    job_name: str,
    timeout_minutes: int,
) -> str:
    """Poll the job's latest execution until it completes or times out."""
    jobs_client = _get_jobs_client(service_account_key_file)
    full_name = f"projects/{project}/locations/{location}/jobs/{job_name}"

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 5
    counter = 0
    execution_name = None

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for Cloud Run job '{job_name}' after {timeout_minutes} minutes"
            )

        try:
            # First, resolve the execution name from the job if we haven't yet
            if not execution_name:
                job = jobs_client.get_job(name=full_name)
                latest = job.latest_created_execution
                if latest and latest.name:
                    short_name = latest.name
                    # The ExecutionReference.name is just the short name (e.g. "job-abc123"),
                    # not the full resource path. Build the full path ourselves.
                    if short_name.startswith("projects/"):
                        execution_name = short_name
                    else:
                        execution_name = f"{full_name}/executions/{short_name}"
                    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                    print(f"{timestamp} 🔍 [{job_name}] Execution: {execution_name}")
                else:
                    if counter == 0 or counter % 6 == 0:
                        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                        print(f"{timestamp} ⏳ [{job_name}] Waiting for execution to start...")
                    counter += 1
                    time.sleep(sleep_interval)
                    continue

            # Poll execution via REST API directly (avoids SDK parsing issues)
            data = _poll_execution_rest(service_account_key_file, execution_name)

            completion_time = data.get("completionTime")
            reconciling = data.get("reconciling", False)
            succeeded = data.get("succeededCount", 0)
            failed = data.get("failedCount", 0)
            running = data.get("runningCount", 0)

            if completion_time or (not reconciling and not running and (succeeded or failed)):
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")

                if failed > 0:
                    print(
                        f"{timestamp} ❌ [{job_name}] Job failed "
                        f"(succeeded: {succeeded}, failed: {failed}, "
                        f"{elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"FAILED (succeeded: {succeeded}, failed: {failed})"
                else:
                    print(
                        f"{timestamp} ✅ [{job_name}] Job completed successfully "
                        f"(tasks succeeded: {succeeded}, {elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"SUCCEEDED (tasks: {succeeded}, {elapsed_min}m {elapsed_sec}s)"

            # Still running
            if counter == 0 or counter % 6 == 0:
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(
                    f"{timestamp} 🔄 [{job_name}] Running... "
                    f"(running: {running}, succeeded: {succeeded}, "
                    f"{elapsed_min}m {elapsed_sec}s elapsed)"
                )

        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} ⚠️ [{job_name}] Error checking status: {str(e)[:80]}...")

        counter += 1
        time.sleep(sleep_interval)


def list_cloud_run_jobs(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """List all Cloud Run Jobs in the specified project and location."""

    client = _get_jobs_client(service_account_key_file)
    parent = f"projects/{project}/locations/{location}"

    print(f"\n🔍 Listing Cloud Run Jobs in {parent}")

    jobs = list(client.list_jobs(parent=parent))

    if not jobs:
        print("No Cloud Run Jobs found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(jobs)} JOB(S)")
    print(f"{'='*80}")

    for i, job in enumerate(jobs, 1):
        name = job.name.split("/")[-1]
        reconciling = "🔄 Reconciling" if job.reconciling else "✅ Ready"

        print(f"\n[{i}/{len(jobs)}] 🐳 {name}")
        print(f"{'-'*50}")
        print(f"   {reconciling}")
        if job.latest_created_execution:
            exec_name = job.latest_created_execution.name.split("/")[-1]
            print(f"   Latest Execution: {exec_name}")
            if job.latest_created_execution.completion_time:
                print(f"   Completed At: {job.latest_created_execution.completion_time}")
        if job.create_time:
            print(f"   Created: {job.create_time}")
        if job.update_time:
            print(f"   Last Updated: {job.update_time}")

    print(f"\n{'='*80}\n")
