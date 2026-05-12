# DinoAI Agents (GraphQL API)

## Overview

{% hint style="info" %}

* This feature is available on workspaces with **DinoAI programmable agents** enabled.
* Requests must be authenticated with a Paradime API key that has access to the DinoAI Agents API.
* Use the GraphQL endpoint shown to you when you generate your API key — it is region-specific (for example `api.paradime.io` or `api.us.paradime.io`).
  {% endhint %}

The DinoAI Agents GraphQL API lets you drive **DinoAI programmable agents** directly over HTTP.

This API offers a comprehensive set of operations to trigger agent runs from YAML-defined agents, send ad-hoc prompts, follow up on a live session with new messages, and poll for run state.

## Trigger an agent run

Triggers a DinoAI programmable agent run. At least one of `agent` or `message` must be provided.

{% tabs %}
{% tab title="Args" %}
**`agent`** *`(String)`*: Name of the YAML-defined agent to load (matches the file name under `.dinoai/agents/` without the `.yml` extension).

**`message`** *`(String)`*: Custom prompt appended to the agent's context. When only `agent` is provided, the run starts with the agent's role/goal/backstory.

**`slack`** *`(DinoAiAgentSlackInput)`*: Optional Slack routing overrides for this run.

* **`channel`** *`(String)`*: Override the Slack channel (e.g. `"#alerts"`).
* **`thread`** *`(String)`*: Override the Slack thread timestamp.
{% endtab %}

{% tab title="Returns" %}
*`TriggerDinoaiAgentRunPayload`*

* **`ok`** *`(Boolean)`*: Whether the run was accepted.
* **`agentSessionId`** *`(String)`*: The session ID for the new run. Use this for follow-ups and polling.
* **`status`** *`(String)`*: Initial status of the run (one of `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`).
{% endtab %}
{% endtabs %}

```graphql
mutation TriggerDinoaiAgentRun(
  $agent: String
  $message: String
  $slack: DinoAiAgentSlackInput
) {
  triggerDinoaiAgentRun(agent: $agent, message: $message, slack: $slack) {
    ok
    agentSessionId
    status
  }
}
```

```json
{
  "agent": "data-quality-checker",
  "message": "Check stg_orders for missing not_null tests.",
  "slack": {
    "channel": "#alerts",
    "thread": "1714142436.001200"
  }
}
```

## Send a follow-up message

Sends a follow-up message to an active DinoAI agent session. The agent pod stays alive for up to 24 hours since the last message; follow-ups resume the same conversation with full context.

{% tabs %}
{% tab title="Args" %}
**`agentSessionId`** *`(String!)`*: The session ID of the running agent.

**`message`** *`(String!)`*: The follow-up message to send.
{% endtab %}

{% tab title="Returns" %}
*`SendDinoaiAgentMessagePayload`*

* **`ok`** *`(Boolean)`*: Whether the message was accepted.
* **`agentSessionId`** *`(String)`*: The session ID the message was attached to.
* **`status`** *`(String)`*: Current status of the run (one of `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`).
{% endtab %}
{% endtabs %}

```graphql
mutation SendDinoaiAgentMessage(
  $agentSessionId: String!
  $message: String!
) {
  sendDinoaiAgentMessage(agentSessionId: $agentSessionId, message: $message) {
    ok
    agentSessionId
    status
  }
}
```

```json
{
  "agentSessionId": "xwzdneft6emspe0f",
  "message": "Now also check stg_customers and post a summary to Slack."
}
```

## Get an agent run

Fetches the current state of a DinoAI agent run.

{% tabs %}
{% tab title="Args" %}
**`agentSessionId`** *`(String!)`*: The session ID returned by `triggerDinoaiAgentRun` or `sendDinoaiAgentMessage`.
{% endtab %}

{% tab title="Returns" %}
*`DinoaiAgentRun`*

* **`ok`** *`(Boolean)`*: Whether the lookup succeeded.
* **`status`** *`(String)`*: One of `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`.
* **`messages`** *`([DinoaiAgentMessage])`*: Full conversation transcript. Each message has:
  * **`ts`** *`(String)`*: Message timestamp.
  * **`role`** *`(String)`*: Author role (e.g. `user`, `assistant`).
  * **`content`** *`(String)`*: Message body.
* **`childSessionIds`** *`([String])`*: Session IDs of sub-agents spawned during the run.
* **`workspaceUid`** *`(String)`*: Workspace the run belongs to.
{% endtab %}
{% endtabs %}

```graphql
query DinoaiAgentRun($id: String!) {
  dinoaiAgentRun(agentSessionId: $id) {
    ok
    status
    messages {
      ts
      role
      content
    }
    childSessionIds
    workspaceUid
  }
}
```

```json
{
  "id": "xwzdneft6emspe0f"
}
```

## Example: curl

```bash
curl -X POST "$PARADIME_API_ENDPOINT" \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: $PARADIME_API_KEY" \
  -H "X-API-SECRET: $PARADIME_API_SECRET" \
  -d '{
    "query": "mutation TriggerDinoaiAgentRun($agent: String, $message: String) { triggerDinoaiAgentRun(agent: $agent, message: $message) { ok agentSessionId status } }",
    "variables": {
      "agent": "data-quality-checker",
      "message": "Focus on stg_orders"
    }
  }'
```


---

# Agent Instructions: Querying This Documentation

If you need additional information that is not directly available in this page, you can query the documentation dynamically by asking a question.

Perform an HTTP GET request on the current page URL with the `ask` query parameter:

```
GET https://docs.paradime.io/app-help/developers/graphql-api/dinoai-agents.md?ask=<question>
```

The question should be specific, self-contained, and written in natural language.
The response will contain a direct answer to the question and relevant excerpts and sources from the documentation.

Use this mechanism when the answer is not explicitly present in the current page, you need clarification or additional context, or you want to retrieve related documentation sections.
