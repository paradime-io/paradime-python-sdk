from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from paradime.cli import console


def trigger_zapier_webhooks(
    *,
    webhook_urls: List[str],
    payload: Optional[str] = None,
) -> List[str]:
    """
    Trigger multiple Zapier webhooks in parallel.

    Args:
        webhook_urls: List of Zapier webhook URLs to trigger
        payload: Optional JSON payload string to send to each webhook

    Returns:
        List of status keywords: SUCCESS or FAILED for each webhook
    """
    parsed_payload: dict | None = None
    if payload:
        try:
            parsed_payload = json.loads(payload)
        except json.JSONDecodeError as e:
            console.error(f"Invalid JSON payload: {e}")
            raise

    futures = []
    results: List[str] = []

    with ThreadPoolExecutor() as executor:
        for webhook_url in set(webhook_urls):
            futures.append(
                (
                    webhook_url,
                    executor.submit(
                        _trigger_single_webhook,
                        webhook_url=webhook_url,
                        payload=parsed_payload,
                    ),
                )
            )

        webhook_results = []
        for webhook_url, future in futures:
            result = future.result(timeout=120)
            webhook_results.append((webhook_url, result))
            results.append(result["status"])

        console.table(
            columns=["Webhook URL", "Status", "Request ID"],
            rows=[
                (
                    url,
                    result["status"],
                    result.get("request_id", "—"),
                )
                for url, result in webhook_results
            ],
            title="Trigger Results",
        )

    return results


def _trigger_single_webhook(
    *,
    webhook_url: str,
    payload: dict | None = None,
) -> dict:
    """
    Trigger a single Zapier webhook.

    Args:
        webhook_url: The full Zapier webhook URL
        payload: Optional parsed JSON payload to send

    Returns:
        Dict with status, request_id keys
    """
    console.debug(f"[{webhook_url}] Triggering webhook...")

    try:
        response = requests.post(
            webhook_url,
            json=payload or {},
            headers={"Content-Type": "application/json"},
        )

        if response.status_code in (200, 202):
            response_data = {}
            try:
                response_data = response.json()
            except Exception:
                pass

            request_id = response_data.get("request_id", response_data.get("id", "—"))
            console.debug(f"[{webhook_url}] Success (request_id={request_id})")
            return {"status": "SUCCESS", "request_id": str(request_id)}
        else:
            console.debug(
                f"[{webhook_url}] Failed with HTTP {response.status_code}: {response.text}"
            )
            return {"status": "FAILED", "request_id": "—"}

    except Exception as e:
        console.debug(f"[{webhook_url}] Request error: {str(e)[:100]}")
        return {"status": "FAILED", "request_id": "—"}
