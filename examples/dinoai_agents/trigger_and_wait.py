import os

from paradime import Paradime

paradime = Paradime(
    api_endpoint=os.environ["PARADIME_API_ENDPOINT"],
    api_key=os.environ["PARADIME_API_KEY"],
    api_secret=os.environ["PARADIME_API_SECRET"],
)

# Trigger a named agent and block until it completes
run = paradime.dinoai_agents.trigger_run_and_wait(
    agent="data-quality-checker",
    message="Find models withouth tests and generate missing tests for one model",
)

print(f"Status: {run.status}")
for msg in run.messages:
    print(f"[{msg.role}] {msg.content}")

# Child sessions (sub-agents spawned during the run)
for child_id in run.child_session_ids:
    child_run = paradime.dinoai_agents.get_run(agent_session_id=child_id)
    print(f"Child {child_id} status: {child_run.status}")
