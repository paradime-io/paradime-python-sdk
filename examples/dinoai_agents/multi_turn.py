"""
Multi-turn conversation with a DinoAI programmable agent.

Flow:
  1. Trigger a run with an opening message.
  2. Wait for the agent to finish its first turn (polling).
  3. Print every message as it appears.
  4. Send a follow-up message.
  5. Wait for the agent to finish again, then print the new messages.
"""
import os
import time

from paradime import Paradime
from paradime.apis.dinoai_agents.types import DinoaiAgentRunStatus

# ── Client setup ─────────────────────────────────────────────────────────────

paradime = Paradime(
    api_endpoint=os.environ["PARADIME_API_ENDPOINT"],
    api_key=os.environ["PARADIME_API_KEY"],
    api_secret=os.environ["PARADIME_API_SECRET"],
)

POLL_INTERVAL = 5  # seconds


# ── Helper: poll until the agent finishes, streaming new messages ─────────────

def wait_for_turn(session_id: str, seen: int = 0) -> tuple:
    """
    Block until the run reaches COMPLETED or FAILED.
    Prints any new messages as they arrive.
    Returns (final_run, new_seen_count).
    """
    while True:
        run = paradime.dinoai_agents.get_run(agent_session_id=session_id)

        # Print messages we haven't seen yet
        for msg in run.messages[seen:]:
            print(f"\n[{msg.role.upper()}]\n{msg.content}\n")
        seen = len(run.messages)

        if run.status == DinoaiAgentRunStatus.COMPLETED:
            return run, seen
        if run.status == DinoaiAgentRunStatus.FAILED:
            raise RuntimeError(f"Agent run failed. Session: {session_id}")

        print(f"  … status: {run.status.value}, waiting {POLL_INTERVAL}s")
        time.sleep(POLL_INTERVAL)


# ── Turn 1: open the conversation ─────────────────────────────────────────────

print("Starting agent run…")
result = paradime.dinoai_agents.trigger_run(
    agent="data-quality-checker",
    message="Check stg_orders for missing not_null tests.",
)

session_id = result.agent_session_id
print(f"Session ID: {session_id}")
print("-" * 60)

run, seen = wait_for_turn(session_id)

# ── Turn 2: send a follow-up ──────────────────────────────────────────────────

print("-" * 60)
follow_up = "Great. Now also check stg_customers and post a summary to Slack."
print(f"[YOU]\n{follow_up}\n")

paradime.dinoai_agents.send_message(
    agent_session_id=session_id,
    message=follow_up,
)

run, seen = wait_for_turn(session_id, seen)

# ── Done ──────────────────────────────────────────────────────────────────────

print("-" * 60)
print(f"Conversation finished. Final status: {run.status.value}")
if run.child_session_ids:
    print(f"Child sessions spawned: {run.child_session_ids}")
