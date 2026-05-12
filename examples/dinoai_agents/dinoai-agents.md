# DinoAI Agents

## Overview

{% hint style="info" %}

* This feature is available on workspaces with **DinoAI programmable agents** enabled.
* Your API keys ***must*** have access to the DinoAI Agents API.
  {% endhint %}

The DinoAI Agents module lets you drive **DinoAI programmable agents** from Python.

This module offers a comprehensive set of tools to trigger agent runs from YAML-defined agents, send ad-hoc prompts, follow up on a live session with new messages, poll for run state, and block until a run completes.

## Trigger an agent run

Triggers a DinoAI programmable agent run. At least one of `agent` or `message` must be provided.

{% tabs %}
{% tab title="Args" %}
**`agent`** *`(Optional[str])`*: Name of the YAML-defined agent to load (matches the file name under `.dinoai/agents/` without the `.yml` extension).

**`message`** *`(Optional[str])`*: Custom prompt appended to the agent's context. When only `agent` is provided, the run starts with the agent's role/goal/backstory.

**`slack_channel`** *`(Optional[str])`*: Override the Slack channel for this run (e.g. `"#alerts"`).

**`slack_thread`** *`(Optional[str])`*: Override the Slack thread timestamp for this run.
{% endtab %}

{% tab title="Returns" %}
*`DinoaiAgentTriggerResult`*: Contains `ok`, `agent_session_id`, and `status`.
{% endtab %}
{% endtabs %}

```python
# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Trigger a named agent with an opening message
result = paradime.dinoai_agents.trigger_run(
    agent="data-quality-checker",
    message="Check stg_orders for missing not_null tests.",
)

print(result.agent_session_id)
```

## Trigger an agent run and wait for completion

Triggers a DinoAI agent run and blocks until it reaches `COMPLETED` or `FAILED`.

{% tabs %}
{% tab title="Args" %}
**`agent`** *`(Optional[str])`*: Name of the YAML-defined agent to load.

**`message`** *`(Optional[str])`*: Custom prompt appended to the agent's context.

**`slack_channel`** *`(Optional[str])`*: Override the Slack channel for this run.

**`slack_thread`** *`(Optional[str])`*: Override the Slack thread timestamp for this run.

**`timeout`** *`(int)`*: Maximum seconds to wait before raising `TimeoutError`. Defaults to `3600`.

**`poll_interval`** *`(int)`*: Seconds between status polls. Defaults to `10`.
{% endtab %}

{% tab title="Returns" %}
*`DinoaiAgentRun`*: The final run state, including `status`, all `messages`, `child_session_ids`, and `workspace_uid`.

Raises `DinoaiAgentRunFailedException` if the run finishes with status `FAILED`, and `TimeoutError` if the run does not complete within `timeout` seconds.
{% endtab %}
{% endtabs %}

```python
# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Trigger a named agent and block until it completes
run = paradime.dinoai_agents.trigger_run_and_wait(
    agent="data-quality-checker",
    message="Focus on stg_orders",
)

print(f"Status: {run.status}")
for msg in run.messages:
    print(f"[{msg.role}] {msg.content}")
```

## Send a follow-up message

Sends a follow-up message to an active DinoAI agent session. The agent pod stays alive for up to 24 hours since the last message; follow-ups resume the same conversation with full context.

{% tabs %}
{% tab title="Args" %}
**`agent_session_id`** *`(str)`*: The session ID of the running agent.

**`message`** *`(str)`*: The follow-up message to send.
{% endtab %}

{% tab title="Returns" %}
*`DinoaiAgentTriggerResult`*: Contains `ok`, `agent_session_id`, and `status`.
{% endtab %}
{% endtabs %}

```python
# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Send a follow-up to an existing session
paradime.dinoai_agents.send_message(
    agent_session_id="xwzdneft6emspe0f",
    message="Now also check stg_customers and post a summary to Slack.",
)
```

## Get an agent run

Fetches the current state of a DinoAI agent run.

{% tabs %}
{% tab title="Args" %}
**`agent_session_id`** *`(str)`*: The session ID returned by `trigger_run` or `send_message`.
{% endtab %}

{% tab title="Returns" %}
*`DinoaiAgentRun`*: Contains `ok`, `status` (one of `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`), `messages` (each with `ts`, `role`, `content`), `child_session_ids` (sub-agents spawned during the run), and `workspace_uid`.
{% endtab %}
{% endtabs %}

```python
# First party modules
from paradime import Paradime
from paradime.apis.dinoai_agents.types import DinoaiAgentRunStatus

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Poll the current state of a session
run = paradime.dinoai_agents.get_run(agent_session_id="xwzdneft6emspe0f")

print(f"Status: {run.status}")
for msg in run.messages:
    print(f"[{msg.role}] {msg.content}")

# Inspect child sessions (sub-agents spawned during the run)
for child_id in run.child_session_ids:
    child_run = paradime.dinoai_agents.get_run(agent_session_id=child_id)
    print(f"Child {child_id} status: {child_run.status}")
```


---

# Agent Instructions: Querying This Documentation

If you need additional information that is not directly available in this page, you can query the documentation dynamically by asking a question.

Perform an HTTP GET request on the current page URL with the `ask` query parameter:

```
GET https://docs.paradime.io/app-help/developers/python-sdk/modules/dinoai-agents.md?ask=<question>
```

The question should be specific, self-contained, and written in natural language.
The response will contain a direct answer to the question and relevant excerpts and sources from the documentation.

Use this mechanism when the answer is not explicitly present in the current page, you need clarification or additional context, or you want to retrieve related documentation sections.
