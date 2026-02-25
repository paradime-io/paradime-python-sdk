import datetime
import logging
import time
from typing import Any, Dict, List, Optional

from paradime.core.scripts.gcp_utils import get_gcp_credentials

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_job_client(service_account_key_file: str, region: str) -> Any:
    from google.cloud import dataproc_v1

    credentials = get_gcp_credentials(service_account_key_file)
    return dataproc_v1.JobControllerClient(
        credentials=credentials,
        client_options={"api_endpoint": f"https://{region}-dataproc.googleapis.com"},
        transport="rest",
    )


def _get_cluster_client(service_account_key_file: str, region: str) -> Any:
    from google.cloud import dataproc_v1

    credentials = get_gcp_credentials(service_account_key_file)
    return dataproc_v1.ClusterControllerClient(
        credentials=credentials,
        client_options={"api_endpoint": f"https://{region}-dataproc.googleapis.com"},
        transport="rest",
    )


def _build_job_config(
    *,
    cluster_name: str,
    job_type: str,
    main_file: Optional[str],
    main_class: Optional[str],
    args: Optional[List[str]],
    job_file: Optional[str],
) -> Dict[str, Any]:
    """Build a Dataproc job configuration dict based on job type."""
    job: Dict[str, Any] = {
        "placement": {"cluster_name": cluster_name},
    }

    if job_type == "pyspark":
        if not main_file:
            raise Exception("--main-file is required for pyspark jobs")
        pyspark_config: Dict[str, Any] = {"main_python_file_uri": main_file}
        if args:
            pyspark_config["args"] = list(args)
        job["pyspark_job"] = pyspark_config

    elif job_type == "spark":
        spark_config: Dict[str, Any] = {}
        if main_class:
            spark_config["main_class"] = main_class
        if main_file:
            spark_config["main_jar_file_uri"] = main_file
        if args:
            spark_config["args"] = list(args)
        if not main_class and not main_file:
            raise Exception("--main-class or --main-file is required for spark jobs")
        job["spark_job"] = spark_config

    elif job_type == "hive":
        if not job_file:
            raise Exception("--job-file is required for hive jobs")
        job["hive_job"] = {"query_file_uri": job_file}

    elif job_type == "spark-sql":
        if not job_file:
            raise Exception("--job-file is required for spark-sql jobs")
        job["spark_sql_job"] = {"query_file_uri": job_file}

    elif job_type == "pig":
        if not job_file:
            raise Exception("--job-file is required for pig jobs")
        job["pig_job"] = {"query_file_uri": job_file}

    elif job_type == "presto":
        if not job_file:
            raise Exception("--job-file is required for presto jobs")
        job["presto_job"] = {"query_file_uri": job_file}

    else:
        raise Exception(f"Unsupported job type: {job_type}")

    return job


def trigger_dataproc_jobs(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    cluster_name: str,
    job_type: str,
    main_file: Optional[str] = None,
    main_class: Optional[str] = None,
    args: Optional[List[str]] = None,
    job_file: Optional[str] = None,
    wait_for_completion: bool = True,
    timeout_minutes: int = 1440,
) -> List[str]:
    """Submit a Dataproc job to a cluster."""

    print(f"\n{'='*60}")
    print("🚀 SUBMITTING DATAPROC JOB")
    print(f"{'='*60}")
    print(f"   Cluster: {cluster_name}")
    print(f"   Job Type: {job_type}")
    if main_file:
        print(f"   Main File: {main_file}")
    if main_class:
        print(f"   Main Class: {main_class}")
    if job_file:
        print(f"   Job File: {job_file}")
    print(f"{'-'*60}")

    result = _submit_dataproc_job(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
        cluster_name=cluster_name,
        job_type=job_type,
        main_file=main_file,
        main_class=main_class,
        args=args,
        job_file=job_file,
        wait_for_completion=wait_for_completion,
        timeout_minutes=timeout_minutes,
    )

    print(f"\n{'='*80}")
    print("📊 RESULT")
    print(f"{'='*80}")

    if "DONE" in result or "SUCCESS" in result:
        print(f"   ✅ {result}")
    elif "FAILED" in result or "ERROR" in result:
        print(f"   ❌ {result}")
    else:
        print(f"   ℹ️ {result}")

    print(f"{'='*80}\n")

    return [result]


def _submit_dataproc_job(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
    cluster_name: str,
    job_type: str,
    main_file: Optional[str],
    main_class: Optional[str],
    args: Optional[List[str]],
    job_file: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> str:
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")

    client = _get_job_client(service_account_key_file, location)

    job_config = _build_job_config(
        cluster_name=cluster_name,
        job_type=job_type,
        main_file=main_file,
        main_class=main_class,
        args=args,
        job_file=job_file,
    )

    print(f"{timestamp} 🚀 [{cluster_name}] Submitting {job_type} job...")

    result = client.submit_job(
        project_id=project,
        region=location,
        job=job_config,
    )
    job_id = result.reference.job_id

    print(f"{timestamp} ✅ [{cluster_name}] Job submitted with ID: {job_id}")
    console_url = (
        f"https://console.cloud.google.com/dataproc/jobs/{job_id}"
        f"?region={location}&project={project}"
    )
    print(f"{timestamp} 🔗 [{cluster_name}] Console: {console_url}")

    if not wait_for_completion:
        return f"TRIGGERED (job_id: {job_id})"

    print(f"{timestamp} ⏳ [{cluster_name}] Monitoring job progress...")
    return _wait_for_dataproc_job(
        client=client,
        project=project,
        location=location,
        job_id=job_id,
        cluster_name=cluster_name,
        timeout_minutes=timeout_minutes,
    )


def _wait_for_dataproc_job(
    *,
    client: Any,
    project: str,
    location: str,
    job_id: str,
    cluster_name: str,
    timeout_minutes: int,
) -> str:
    from google.cloud import dataproc_v1

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    sleep_interval = 10
    counter = 0

    terminal_states = {
        dataproc_v1.JobStatus.State.DONE,
        dataproc_v1.JobStatus.State.ERROR,
        dataproc_v1.JobStatus.State.CANCELLED,
    }

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise Exception(
                f"Timeout waiting for Dataproc job '{job_id}' after {timeout_minutes} minutes"
            )

        try:
            job = client.get_job(
                project_id=project,
                region=location,
                job_id=job_id,
            )
            state = job.status.state

            if counter == 0 or counter % 6 == 0:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                state_name = dataproc_v1.JobStatus.State(state).name
                print(
                    f"{timestamp} 🔄 [{cluster_name}/{job_id}] State: {state_name} "
                    f"({elapsed_min}m {elapsed_sec}s elapsed)"
                )

            if state in terminal_states:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                elapsed_min = int(elapsed // 60)
                elapsed_sec = int(elapsed % 60)
                state_name = dataproc_v1.JobStatus.State(state).name

                if state == dataproc_v1.JobStatus.State.DONE:
                    print(
                        f"{timestamp} ✅ [{cluster_name}/{job_id}] Completed successfully "
                        f"({elapsed_min}m {elapsed_sec}s)"
                    )
                    return f"DONE ({elapsed_min}m {elapsed_sec}s)"
                elif state == dataproc_v1.JobStatus.State.ERROR:
                    details = job.status.details if job.status.details else "Unknown error"
                    print(f"{timestamp} ❌ [{cluster_name}/{job_id}] Job failed: {details}")
                    return f"ERROR: {details}"
                else:
                    print(f"{timestamp} ⚠️ [{cluster_name}/{job_id}] Job {state_name}")
                    return "CANCELLED"

        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp} ⚠️ [{cluster_name}/{job_id}] Error checking status: {str(e)[:80]}..."
            )

        counter += 1
        time.sleep(sleep_interval)


def list_dataproc_clusters(
    *,
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """List all Dataproc clusters."""
    from google.cloud import dataproc_v1

    client = _get_cluster_client(service_account_key_file, location)

    print(f"\n🔍 Listing Dataproc clusters in project '{project}', region '{location}'")

    clusters = list(
        client.list_clusters(
            request=dataproc_v1.ListClustersRequest(
                project_id=project,
                region=location,
            )
        )
    )

    if not clusters:
        print("No Dataproc clusters found.")
        return

    print(f"\n{'='*80}")
    print(f"📋 FOUND {len(clusters)} CLUSTER(S)")
    print(f"{'='*80}")

    for i, cluster in enumerate(clusters, 1):
        state_name = dataproc_v1.ClusterStatus.State(cluster.status.state).name
        state_emoji = (
            "✅"
            if state_name == "RUNNING"
            else (
                "🔄"
                if state_name in ("CREATING", "UPDATING", "STARTING")
                else "⏸️" if state_name == "STOPPED" else "❌" if state_name == "ERROR" else "❓"
            )
        )

        master_type = "N/A"
        num_workers = 0
        if cluster.config and cluster.config.master_config:
            master_type = cluster.config.master_config.machine_type_uri or "N/A"
        if cluster.config and cluster.config.worker_config:
            num_workers = cluster.config.worker_config.num_instances or 0

        print(f"\n[{i}/{len(clusters)}] 🖥️ {cluster.cluster_name}")
        print(f"{'-'*50}")
        print(f"   {state_emoji} State: {state_name}")
        print(f"   Master Type: {master_type}")
        print(f"   Workers: {num_workers}")
        if cluster.status.state_start_time:
            print(f"   State Since: {cluster.status.state_start_time}")
        console_url = (
            f"https://console.cloud.google.com/dataproc/clusters/{cluster.cluster_name}"
            f"?region={location}&project={project}"
        )
        print(f"   🔗 Console: {console_url}")

    print(f"\n{'='*80}\n")
