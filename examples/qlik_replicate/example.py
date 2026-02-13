"""
Example usage of Qlik Replicate task triggers.

This example demonstrates how to trigger Qlik Replicate tasks using environment variables
for authentication.
"""

import os

from paradime.core.scripts.qlik_replicate import (
    list_qlik_replicate_tasks,
    stop_qlik_replicate_tasks,
    trigger_qlik_replicate_tasks,
)

# Get credentials from environment variables
QLIK_HOST = os.getenv("QLIK_HOST")  # e.g., "https://your-qlik-server.com"
QLIK_USERNAME = os.getenv("QLIK_USERNAME")
QLIK_PASSWORD = os.getenv("QLIK_PASSWORD")
QLIK_SERVER_NAME = os.getenv("QLIK_SERVER_NAME")  # Replicate server name

# Example 1: List all tasks
print("Example 1: Listing all Qlik Replicate tasks")
print("=" * 60)
list_qlik_replicate_tasks(
    host=QLIK_HOST,
    username=QLIK_USERNAME,
    password=QLIK_PASSWORD,
    server_name=QLIK_SERVER_NAME,
)

# Example 2: Trigger multiple tasks with RESUME_PROCESSING
print("\nExample 2: Triggering tasks with RESUME_PROCESSING")
print("=" * 60)
results = trigger_qlik_replicate_tasks(
    host=QLIK_HOST,
    username=QLIK_USERNAME,
    password=QLIK_PASSWORD,
    server_name=QLIK_SERVER_NAME,
    task_names=["my_task_1", "my_task_2"],
    run_option="RESUME_PROCESSING",  # Resume from where it left off
    wait_for_completion=True,
    timeout_minutes=60,
)
print(f"Results: {results}")

# Example 3: Trigger task with RELOAD_TARGET (full reload)
print("\nExample 3: Triggering task with RELOAD_TARGET")
print("=" * 60)
results = trigger_qlik_replicate_tasks(
    host=QLIK_HOST,
    username=QLIK_USERNAME,
    password=QLIK_PASSWORD,
    server_name=QLIK_SERVER_NAME,
    task_names=["my_task_3"],
    run_option="RELOAD_TARGET",  # Full reload of target tables
    wait_for_completion=False,  # Don't wait for completion
    timeout_minutes=60,
)
print(f"Results: {results}")

# Example 4: Stop tasks
print("\nExample 4: Stopping tasks")
print("=" * 60)
stop_results = stop_qlik_replicate_tasks(
    host=QLIK_HOST,
    username=QLIK_USERNAME,
    password=QLIK_PASSWORD,
    server_name=QLIK_SERVER_NAME,
    task_names=["my_task_1"],
    timeout_seconds=30,
)
print(f"Stop Results: {stop_results}")

# Example 5: Trigger with error recovery
print("\nExample 5: Triggering task with RECOVER option")
print("=" * 60)
results = trigger_qlik_replicate_tasks(
    host=QLIK_HOST,
    username=QLIK_USERNAME,
    password=QLIK_PASSWORD,
    server_name=QLIK_SERVER_NAME,
    task_names=["my_task_4"],
    run_option="RECOVER",  # Recover from error state
    wait_for_completion=True,
    timeout_minutes=30,
)
print(f"Results: {results}")
