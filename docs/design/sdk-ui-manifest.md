# SDK UI Manifest Design

## Problem

Today, integrations in the SDK define their interface in three disconnected ways:

1. **CLI** - Click decorators in `paradime/cli/integrations/*.py`
2. **Functions** - Python function signatures in `paradime/core/scripts/*.py`
3. **UI** - Does not exist yet; the frontend has no way to dynamically render forms for integration commands

When a new integration is added (e.g., Fivetran), a developer defines Click options, writes the core function, and then separately the frontend team hardcodes a form. There is no single source of truth tying CLI options, function parameters, and UI fields together.

## Goal

A single, standardised manifest per integration that defines:
- What commands the integration exposes
- What fields each command needs (with types, validation, and UI hints)
- How fields relate to each other (conditional visibility, dependencies)
- Which fields need dynamic data resolved at runtime (e.g., "list of connectors")
- Help URLs at both the integration level and individual field level for contextual documentation
- Encoding hints (e.g., base64) so the frontend can encode field values before submission
- Enough information for the CLI, the backend API, and the frontend to all work from the same definition

---

## Architecture Overview

The SDK provides the **schema models** (Pydantic classes) and a **registry** for manifests. The per-integration manifest instances are defined in the **backend repo**, not in the SDK. This keeps the UI-specific definitions (help URLs, field ordering, encoding hints) close to the API layer that serves them and the frontend that consumes them, while the SDK remains focused on core function implementations and the shared type vocabulary.

```
┌─────────────────────────────────────────────────────────────┐
│                     SDK (this repo)                         │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  Schema       │  │  Integration │  │  Integration     │  │
│  │  Models       │  │  Core Funcs  │  │  CLI commands    │  │
│  │  (Pydantic)   │  │  (execute)   │  │  (Click)         │  │
│  └──────┬───────┘  └──────────────┘  └──────────────────┘  │
│         │                                                   │
│  ┌──────▼───────┐                                           │
│  │  Registry     │  ← provides register/lookup/serialize    │
│  └──────┬───────┘    (populated by backend at startup)      │
└─────────┼───────────────────────────────────────────────────┘
          │
          │  SDK is a dependency
          ▼
┌─────────────────────────────────────────────────────────────┐
│                  paradime-backend                            │
│                                                             │
│  ┌──────────────────┐  ┌────────────────────────────────┐  │
│  │  Integration      │  │  /integrations API endpoint    │  │
│  │  Manifests        │  │                                │  │
│  │  (source of truth)│  │  1. loads manifests into       │  │
│  │                   │  │     SDK registry at startup    │  │
│  │  fivetran.py      │  │  2. resolves dynamic fields    │  │
│  │  tableau.py       │──│  3. returns JSON to frontend   │  │
│  │  hightouch.py     │  │                                │  │
│  │  ...              │  │  Dynamic Field Resolver        │  │
│  │                   │  │  - calls list_connectors()     │  │
│  └──────────────────┘  │  - populates dropdown opts     │  │
│                         │  - caches where appropriate    │  │
│                         └───────────┬────────────────────┘  │
└─────────────────────────────────────┼───────────────────────┘
                                      │
                                      │  JSON response
                                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  paradime-frontend                           │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Generic Form Renderer (react-hook-form)              │   │
│  │  - reads field definitions from JSON                  │   │
│  │  - renders inputs, dropdowns, checkboxes, switches    │   │
│  │  - applies conditional visibility rules               │   │
│  │  - handles repeatable field groups                    │   │
│  │  - validates required/optional per schema             │   │
│  │  - renders help_url links on integrations and fields  │   │
│  │  - base64-encodes fields with encode: "base64"        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Part 1: Manifest Schema

Each integration defines a manifest. The manifest is the single source of truth for that integration's identity, commands, and fields.

### 1.1 Integration-Level Metadata

| Property | Type | Description |
|---|---|---|
| `id` | `string` | Unique identifier, e.g. `"fivetran"` |
| `name` | `string` | Display name, e.g. `"Fivetran"` |
| `description` | `string` | Short description shown in UI |
| `icon` | `string` | Icon identifier (from Blueprint icons or a logo URL) |
| `category` | `string` | Grouping category: `"etl"`, `"bi"`, `"orchestration"`, `"data-quality"`, etc. |
| `help_url` | `string \| null` | URL linking to setup documentation for this integration. Frontend renders this as a help icon/link in the integration header (e.g., `"https://docs.paradime.io/integrations/fivetran"`). |
| `auth_fields` | `list[Field]` | Credentials shared across all commands (e.g., api_key, api_secret) |
| `commands` | `list[Command]` | The operations this integration supports |

### 1.2 Command Definition

Each command maps to exactly one core function and one CLI command.

| Property | Type | Description |
|---|---|---|
| `id` | `string` | Unique within the integration, e.g. `"sync"` |
| `name` | `string` | Display name, e.g. `"Trigger Sync"` |
| `description` | `string` | What this command does |
| `type` | `enum` | `"action"` (triggers something) or `"list"` (fetches data) |
| `core_function` | `string` | Dotted path to the Python function, e.g. `"paradime.core.scripts.fivetran.trigger_fivetran_sync"` |
| `fields` | `list[Field]` | The parameters for this command |

### 1.3 Field Definition

This is the core of the UI manifest. Each field describes one parameter with enough metadata for CLI, backend validation, and frontend rendering.

| Property | Type | Description |
|---|---|---|
| `id` | `string` | Field identifier, e.g. `"connector_id"` |
| `label` | `string` | Human-readable label for UI |
| `description` | `string` | Help text (shown as tooltip / CLI help) |
| `type` | `FieldType` | See section 1.4 |
| `required` | `bool` | Whether the field must be filled |
| `default` | `any` | Default value (null if none) |
| `env_var` | `string \| null` | Environment variable that can provide this value |
| `repeatable` | `bool` | Whether the user can add multiple values (maps to Click's `multiple=True`) |
| `validation` | `Validation \| null` | Validation rules (see section 1.5) |
| `depends_on` | `Condition \| null` | Conditional visibility (see section 1.6) |
| `dynamic_options` | `DynamicOptions \| null` | For dropdowns whose values come from a function call (see section 1.7) |
| `group` | `string \| null` | Logical grouping for UI layout (fields with the same group render together) |
| `order` | `int` | Display order within the command (lower = higher) |
| `help_url` | `string \| null` | URL linking to documentation for this specific field. Frontend renders this as a small help icon next to the field label (e.g., `"https://fivetran.com/docs/rest-api/getting-started"` on the API key field). |
| `encode` | `string \| null` | Encoding hint for the frontend. When set to `"base64"`, the frontend must base64-encode the raw field value before submitting it to the backend. Useful for JSON payloads, certificates, or other structured text that needs safe transport. Only applicable to `text` and `textarea` field types. |

### 1.4 Field Types

```
FieldType = "text" | "secret" | "number" | "dropdown" | "checkbox" | "switch" | "textarea"
```

| FieldType | CLI Mapping | UI Rendering | Notes |
|---|---|---|---|
| `text` | `--option VALUE` | `<input type="text">` | General string input |
| `secret` | `--option VALUE` (+ env_var) | `<input type="password">` | Credentials, never logged |
| `number` | `--option VALUE` (type=int) | `<input type="number">` | Integer or float |
| `dropdown` | `--option VALUE` (type=Choice) | `<select>` | Static or dynamic options |
| `checkbox` | `--flag / --no-flag` | `<input type="checkbox">` | Boolean toggle |
| `switch` | `--flag / --no-flag` | `<Switch>` component | Same as checkbox, different visual |
| `textarea` | N/A (UI only) | `<textarea>` | For longer text like JSON payloads. Supports `encode: "base64"` for safe transport of structured content. |

### 1.5 Validation Rules

```
Validation:
  min_length: int | null
  max_length: int | null
  min_value: number | null
  max_value: number | null
  pattern: string | null        # regex
  pattern_message: string | null # user-friendly error for regex failure
```

### 1.6 Conditional Dependencies

A field can depend on another field's value for visibility or required-ness.

```
Condition:
  field: string          # id of the field this depends on
  operator: "eq" | "neq" | "in" | "not_in" | "truthy" | "falsy"
  value: any             # the value to compare against
  effect: "show" | "hide" | "require" | "unrequire"
```

**Example**: Fivetran's `timeout_minutes` should only appear when `wait_for_completion` is true:
```
depends_on:
  field: "wait_for_completion"
  operator: "truthy"
  effect: "show"
```

**Example**: Tableau's `workbook_name` and `datasource_name` are mutually exclusive:
```
# on workbook_name:
depends_on:
  field: "datasource_name"
  operator: "falsy"
  effect: "show"

# on datasource_name:
depends_on:
  field: "workbook_name"
  operator: "falsy"
  effect: "show"
```

For more complex conditions (AND/OR), `depends_on` can accept a list:
```
depends_on:
  all:   # AND - all conditions must be true
    - field: "wait_for_completion"
      operator: "truthy"
    - field: "connector_id"
      operator: "truthy"
  effect: "show"
```

### 1.7 Dynamic Options (Backend-Resolved Dropdowns)

Some dropdowns need their options populated by calling an API (e.g., "list all Fivetran connectors"). The manifest declares the intent; the backend resolves it.

```
DynamicOptions:
  resolver: string          # dotted path to a "list" command's core function
  depends_on_fields: list[string]  # which auth/other fields must be provided first
  label_key: string         # which key in the result to use as the display label
  value_key: string         # which key in the result to use as the submitted value
  refresh_on: list[string]  # re-fetch when these field values change
```

**Example**: Fivetran connector dropdown
```
dynamic_options:
  resolver: "paradime.core.scripts.fivetran.list_fivetran_connectors"
  depends_on_fields: ["api_key", "api_secret"]
  label_key: "name"
  value_key: "connector_id"
```

The frontend does NOT call the resolver directly. The flow is:
1. Frontend sends the auth field values to the backend
2. Backend calls the resolver function with those values
3. Backend returns the resolved options as part of the field definition
4. Frontend renders the dropdown with those options

---

## Part 2: Where Manifests Live

### Key Decision: Manifests Live in the Backend, Not the SDK

The UI manifest definitions for each integration live in `paradime-backend`, **not** in this SDK repo. The SDK provides only the Pydantic schema models (the "vocabulary") and the registry infrastructure.

**Why:**
- The manifests are primarily consumed by the backend API and the frontend — they are UI/API artifacts, not SDK logic.
- Help URLs, field ordering, encoding hints, and other UI-specific metadata change independently of the core Python functions.
- The backend team can add or modify integration manifests without cutting a new SDK release.
- Keeps the SDK focused on what it does well: core function implementations and the CLI.

### SDK Directory Structure (this repo)

```
paradime/
├── integrations/              # NEW - schema models + registry only
│   ├── __init__.py            # Registry: register/lookup/serialize helpers
│   └── _base.py               # Pydantic models for manifest schema
├── cli/
│   └── integrations/          # UNCHANGED - existing Click commands
├── core/
│   └── scripts/               # UNCHANGED - existing core functions
```

### Backend Directory Structure (paradime-backend repo)

```
paradime-backend/
├── integrations/
│   └── manifests/             # Per-integration manifest definitions
│       ├── __init__.py        # Loads all manifests into registry at startup
│       ├── fivetran.py        # Fivetran manifest instance
│       ├── tableau.py         # Tableau manifest instance
│       ├── hightouch.py       # Hightouch manifest instance
│       ├── airbyte.py
│       └── ...
```

Each backend manifest file imports the schema models from the SDK and instantiates them:

```python
# paradime-backend/integrations/manifests/fivetran.py
from paradime.integrations._base import IntegrationManifest, Field, CommandManifest

manifest = IntegrationManifest(
    id="fivetran",
    name="Fivetran",
    help_url="https://docs.paradime.io/integrations/fivetran",
    auth_fields=[
        Field(
            id="api_key",
            label="API Key",
            type="secret",
            required=True,
            help_url="https://fivetran.com/docs/rest-api/getting-started",
            env_var="FIVETRAN_API_KEY",
            description="Your Fivetran API key from account settings",
        ),
        ...
    ],
    commands=[...],
)
```

### Separation of Concerns

| Concern | Lives In | Why |
|---|---|---|
| Schema models (Pydantic) | SDK (`paradime/integrations/_base.py`) | Shared vocabulary: both SDK tests and backend import these |
| Registry class | SDK (`paradime/integrations/__init__.py`) | Reusable register/lookup/serialize logic |
| Core functions | SDK (`paradime/core/scripts/`) | Python implementation of integration actions |
| CLI commands | SDK (`paradime/cli/integrations/`) | Click-based CLI interface |
| **Manifest instances** | **Backend** (`integrations/manifests/`) | **UI-specific definitions, help URLs, field ordering, encoding hints** |
| API endpoints | Backend | Serves manifests as JSON, resolves dynamic fields |
| Form renderer | Frontend | Renders forms from manifest JSON |

---

## Part 3: The Registry

The registry is a lightweight container defined in the SDK. The backend populates it at startup by registering manifest instances.

### Responsibilities

1. **Collection**: Accept manifest registrations from the backend
2. **Lookup**: Get a specific integration or command manifest by ID
3. **Serialization**: Export all manifests as JSON for the backend API to return

### API Surface

```
registry.register(manifest: IntegrationManifest) -> None
registry.list_integrations() -> list[IntegrationManifest]
registry.get_integration(id: str) -> IntegrationManifest
registry.get_command(integration_id: str, command_id: str) -> CommandManifest
registry.to_dict() -> list[dict]   # JSON-serializable export of all manifests
```

### Registration Pattern

The **backend** registers manifests at startup (not the SDK):

```python
# paradime-backend/integrations/manifests/__init__.py
from paradime.integrations import registry
from .fivetran import manifest as fivetran_manifest
from .tableau import manifest as tableau_manifest
from .hightouch import manifest as hightouch_manifest

def load_all_manifests():
    """Called once at backend startup."""
    registry.register(fivetran_manifest)
    registry.register(tableau_manifest)
    registry.register(hightouch_manifest)
    # ... etc
```

This means:
- The SDK registry starts empty — it has no built-in integration knowledge.
- The backend controls which integrations are available by choosing which manifests to register.
- Different backend environments (staging, production) could register different sets of manifests if needed.

---

## Part 4: Backend Integration (paradime-backend)

The backend consumes the SDK as a dependency, defines the per-integration manifest instances, loads them into the registry at startup, and exposes them to the frontend via API.

### 4.1 Endpoints

**GET /api/integrations**
Returns all integration manifests (metadata + commands + fields).
- Excludes `secret`-type field values
- Includes static dropdown options as-is
- Marks dynamic dropdown fields with `options: null` and `options_pending: true`

**POST /api/integrations/{integration_id}/resolve-options**
Resolves dynamic dropdown options for a specific field.
- Request body: `{ "command_id": "sync", "field_id": "connector_id", "context": { "api_key": "...", "api_secret": "..." } }`
- Backend calls the resolver function with the provided context
- Returns: `{ "options": [{ "label": "My Connector", "value": "abc123" }, ...] }`

**POST /api/integrations/{integration_id}/commands/{command_id}/execute**
Executes a command with the provided field values.
- Validates input against the manifest's field definitions
- Calls the core function
- Returns execution result

### 4.2 Dynamic Field Resolution Flow

```
Frontend                    Backend                         SDK
   │                           │                              │
   │                      (startup)                           │
   │                           │  load_all_manifests()        │
   │                           │  → registry.register(...)    │
   │                           │─────────────────────────────>│
   │                           │                              │
   │  GET /integrations        │                              │
   │ ─────────────────────────>│  registry.list_integrations()│
   │                           │─────────────────────────────>│
   │   { integrations: [...],  │                              │
   │     help_urls, encode     │                              │
   │     hints included }      │                              │
   │ <─────────────────────────│                              │
   │                           │                              │
   │  User fills in api_key    │                              │
   │  and api_secret           │                              │
   │                           │                              │
   │  POST /resolve-options    │                              │
   │  { field: connector_id,   │                              │
   │    context: { api_key,    │  resolver_fn(api_key,        │
   │              api_secret }}│  api_secret)                  │
   │ ─────────────────────────>│─────────────────────────────>│
   │                           │                              │
   │   { options: [...] }      │   [connector objects]        │
   │ <─────────────────────────│<─────────────────────────────│
   │                           │                              │
   │  User selects connector,  │                              │
   │  fills remaining fields,  │                              │
   │  submits                  │                              │
   │                           │                              │
   │  POST /execute            │                              │
   │  { connector_id: "abc",   │  trigger_fivetran_sync(      │
   │    force: true, ... }     │    connector_ids=["abc"],    │
   │ ─────────────────────────>│    force=True, ...)          │
   │                           │─────────────────────────────>│
   │   { result: ... }         │                              │
   │ <─────────────────────────│<─────────────────────────────│
```

### 4.3 Backend Caching Strategy

- **Integration manifests**: Cache indefinitely (only change on backend deploy since manifests live in the backend repo)
- **Resolved dynamic options**: Cache with a short TTL (e.g., 60s) keyed by `(resolver, context_hash)`
- Cache invalidation on explicit user request ("refresh" button in UI)

### 4.4 Base64 Encoding Contract

When the backend receives a field value whose manifest declares `encode: "base64"`:
- The frontend has already base64-encoded the raw value before submission.
- The backend **must base64-decode** the value before passing it to the SDK core function.
- This ensures the SDK core functions always receive plain-text values regardless of the transport encoding.
- The encoding is a frontend-to-backend transport concern only — it does not affect how values are stored or logged.

---

## Part 5: Frontend Rendering (react-hook-form)

### 5.1 Generic Form Renderer

The frontend does NOT have integration-specific form code. A single generic renderer interprets the manifest JSON:

```
ManifestFormRenderer
  ├── FieldRenderer (switches on field.type)
  │     ├── TextInput       → type: "text"
  │     ├── SecretInput     → type: "secret"
  │     ├── NumberInput     → type: "number"
  │     ├── DropdownField   → type: "dropdown" (static or dynamic)
  │     ├── CheckboxField   → type: "checkbox"
  │     ├── SwitchField     → type: "switch"
  │     └── TextareaField   → type: "textarea"
  ├── ConditionalWrapper (evaluates depends_on rules)
  ├── RepeatableGroup (renders add/remove buttons for repeatable fields)
  └── FieldGroup (groups fields by group property)
```

### 5.2 Conditional Field Handling

The frontend evaluates `depends_on` rules client-side using `react-hook-form`'s `watch`:

1. `watch` the dependent field
2. Evaluate the condition (eq, neq, truthy, falsy, etc.)
3. Show/hide or require/unrequire the field accordingly
4. When a field is hidden, clear its value from the form state (to avoid submitting stale data)

### 5.3 Dynamic Dropdown Flow

1. Render the dropdown in a "loading" state with a prompt: *"Fill in [depends_on_fields] first"*
2. Once all `depends_on_fields` have values, auto-trigger `POST /resolve-options`
3. Populate the dropdown with the returned options
4. If the user changes a field listed in `refresh_on`, re-fetch

### 5.4 Repeatable Fields

For fields with `repeatable: true`:
1. Render with an "Add" button
2. Each instance gets its own row with a "Remove" button
3. Submitted as an array of values
4. Minimum 1 entry if the field is required

### 5.5 Help URLs

Help URLs provide contextual documentation links at two levels:

**Integration-level `help_url`:**
- Rendered as a help icon or "Learn more" link in the integration header/card.
- Links to setup documentation (e.g., how to configure Fivetran credentials).
- Example: A small `(?)` icon next to "Fivetran" that opens the Paradime docs page for Fivetran setup.

**Field-level `help_url`:**
- Rendered as a small help icon next to the field label.
- Links to documentation specific to that field (e.g., "Where to find your API key" in the provider's docs).
- Example: A `(?)` icon next to the "API Key" label that opens `https://fivetran.com/docs/rest-api/getting-started`.

Both are optional — if `help_url` is `null`, no help link is rendered.

### 5.6 Base64 Encoding for Text Fields

When a field has `encode: "base64"`:
1. The field renders normally as a `<textarea>` or `<input type="text">` — the user types or pastes plain text.
2. On form submission, the frontend base64-encodes the raw value using `btoa()` / `Buffer.from().toString('base64')` before including it in the request payload.
3. This is transparent to the user — they never see the encoded value.
4. Use cases:
   - JSON payloads that would otherwise need escaping
   - Multi-line configuration blocks
   - Certificates or keys with special characters

---

## Part 6: CLI Generation from Manifest

Today CLI commands are hand-written with Click decorators. With the manifest as the source of truth, CLI commands can be auto-generated.

### Approach: Manifest-Driven CLI Builder

A utility function reads a `CommandManifest` and produces a Click command:

```
build_click_command(command_manifest, auth_fields) -> click.Command
```

The builder:
1. Creates `@click.option` for each field, using:
   - `--{field.id}` as the option name
   - `field.env_var` for `envvar=`
   - `field.required` for `required=`
   - `field.default` for `default=`
   - `field.description` for `help=`
   - `multiple=True` if `field.repeatable`
   - `is_flag=True` if `field.type` is `checkbox` or `switch`
   - `type=click.Choice(...)` if `field.type` is `dropdown` with static options
2. Wires the resulting Click command to call `command.core_function` with the collected arguments
3. Handles argument name mapping (CLI uses kebab-case, Python uses snake_case)

**Benefit**: Adding a new integration = writing one manifest + one core function. The CLI and UI are both derived automatically.

---

## Part 7: Concrete Example - Fivetran Manifest

To illustrate how an existing integration would look under this system. Note: this manifest instance lives in the **backend repo**, not the SDK. It imports the schema models from `paradime.integrations._base`.

```
Integration:
  id: fivetran
  name: Fivetran
  description: ELT data pipeline platform
  icon: fivetran
  category: etl
  help_url: "https://docs.paradime.io/integrations/fivetran"   # ← integration-level help

  auth_fields:
    - id: api_key
      label: API Key
      type: secret
      required: true
      env_var: FIVETRAN_API_KEY
      description: "Your Fivetran API key from account settings"
      help_url: "https://fivetran.com/docs/rest-api/getting-started"  # ← field-level help

    - id: api_secret
      label: API Secret
      type: secret
      required: true
      env_var: FIVETRAN_API_SECRET
      description: "Your Fivetran API secret from account settings"
      help_url: "https://fivetran.com/docs/rest-api/getting-started"  # ← field-level help

  commands:
    - id: sync
      name: Trigger Sync
      description: Trigger sync for Fivetran connectors
      type: action
      core_function: paradime.core.scripts.fivetran.trigger_fivetran_sync
      fields:
        - id: connector_id
          label: Connector(s)
          type: dropdown
          required: true
          repeatable: true
          description: The connector(s) to sync
          dynamic_options:
            resolver: paradime.core.scripts.fivetran.list_fivetran_connectors
            depends_on_fields: [api_key, api_secret]
            label_key: name
            value_key: connector_id
          order: 1

        - id: force
          label: Force Restart
          type: switch
          required: false
          default: false
          description: Force restart any ongoing syncs
          order: 2

        - id: wait_for_completion
          label: Wait for Completion
          type: switch
          required: false
          default: true
          description: Wait for syncs to complete before returning
          order: 3

        - id: timeout_minutes
          label: Timeout (minutes)
          type: number
          required: false
          default: 1440
          description: Maximum wait time in minutes
          depends_on:
            field: wait_for_completion
            operator: truthy
            effect: show
          validation:
            min_value: 1
            max_value: 10080
          order: 4

    - id: list_connectors
      name: List Connectors
      description: List all Fivetran connectors with status
      type: list
      core_function: paradime.core.scripts.fivetran.list_fivetran_connectors
      fields:
        - id: group_id
          label: Group ID
          type: text
          required: false
          description: Filter connectors by group
          order: 1
```

### Example: Field with Base64 Encoding

A hypothetical integration command that accepts a JSON configuration payload:

```
- id: config_json
  label: Configuration (JSON)
  type: textarea
  required: true
  description: "Paste the connector configuration as JSON"
  encode: "base64"            # ← frontend base64-encodes before submit
  help_url: "https://docs.example.com/connector-config-format"
  order: 2
```

The user types raw JSON into the textarea. On submit, the frontend encodes it to base64. The backend decodes it back to the original JSON string before passing to the core function.

---

## Part 8: Repeatable Field Groups

Some commands need repeatable **groups** of fields, not just repeatable single values. For example, a hypothetical "multi-sync" command might need pairs of (source, destination).

### Schema Extension

```
RepeatableGroup:
  id: string
  label: string
  description: string
  min_items: int          # default 1
  max_items: int | null   # null = unlimited
  fields: list[Field]     # the fields that repeat as a unit
```

The `fields` list of a command can contain either `Field` or `RepeatableGroup` entries (distinguished by a `kind` discriminator: `"field"` vs `"group"`).

### Frontend Rendering

Each group instance renders as a bordered card with all its fields inside, plus add/remove controls.

---

## Part 9: Versioning and Backward Compatibility

### Manifest Versioning

Each manifest includes a `schema_version` field (semver). The backend and frontend check this version:
- **Minor bump** (1.1 → 1.2): new optional fields added. Frontend ignores unknown fields gracefully.
- **Major bump** (1 → 2): breaking changes. Frontend shows a "please update" message for unsupported versions.

### SDK and Backend Version Coupling

The backend pins a specific SDK version for the schema models and core functions. Since manifests live in the backend:
1. **New/changed manifests** take effect on backend deploy — no SDK release needed.
2. **New schema features** (e.g., adding a new field type) require an SDK release first, then the backend can use them.
3. **No frontend deploy** needed for new integrations — the renderer is generic and interprets the manifest JSON dynamically.

---

## Part 10: Implementation Phases

### Phase 1: Foundation (SDK repo)
- Define the manifest Pydantic models in `paradime/integrations/_base.py`, including `help_url` on `IntegrationManifest` and `Field`, and `encode` on `Field`
- Build the registry in `paradime/integrations/__init__.py`
- Add `to_dict()` serialization methods on all manifest models
- Write unit tests for schema validation and registry operations
- Release SDK version with the new schema

### Phase 2: Manifest Authoring (Backend repo)
- Create `integrations/manifests/` directory in paradime-backend
- Write the manifest for 1-2 integrations (e.g., Fivetran, Hightouch) as a proof of concept
- Include `help_url` links for integration setup docs and key fields
- Include `encode: "base64"` on any fields that need it (e.g., JSON config payloads)
- Wire up manifest loading at backend startup

### Phase 3: Backend API (Backend repo)
- Add the `/integrations` endpoint — serves manifest JSON including `help_url` and `encode` metadata
- Add the `/resolve-options` endpoint
- Add the `/execute` endpoint with base64-decode logic for fields with `encode: "base64"`
- Wire up caching for resolved options

### Phase 4: CLI Generation (SDK repo)
- Build the `build_click_command()` utility
- Verify that generated CLI commands are functionally identical to hand-written ones
- Add tests comparing generated CLI help output to existing

### Phase 5: Frontend Renderer (Frontend repo)
- Build the generic `ManifestFormRenderer` component
- Implement all field type renderers
- Implement conditional visibility
- Implement dynamic dropdown resolution
- Implement repeatable fields / groups
- Implement `help_url` rendering (integration-level and field-level help icons/links)
- Implement base64 encoding on submit for fields with `encode: "base64"`

### Phase 6: Migration
- Convert remaining integrations one by one to the manifest format (in the backend repo)
- Deprecate hand-written CLI commands
- Remove old frontend hardcoded forms

---

## Part 11: Design Constraints and Decisions

| Decision | Chosen Approach | Rationale |
|---|---|---|
| Manifest format | Python (Pydantic models) | Type-safe, validated at import time, IDE autocompletion. JSON is generated from this, not the other way around. |
| Schema models location | SDK (`paradime/integrations/_base.py`) | Shared vocabulary used by both SDK tests and backend. Part of the SDK package. |
| Manifest instances location | Backend repo (`integrations/manifests/`) | UI-specific definitions (help URLs, field ordering, encoding hints) change independently of core functions. No SDK release needed for manifest changes. |
| Help URLs | Optional at integration and field level | Provides contextual documentation without cluttering the UI. Null = no link rendered. |
| Base64 encoding | Frontend encodes, backend decodes | Safe transport for JSON payloads and structured text. Transparent to the user and to SDK core functions. |
| Dynamic options | Backend-resolved, not frontend-direct | Frontend never has credentials. Backend caches and controls access. |
| CLI generation | Auto-generated from manifest | Eliminates drift between CLI and UI definitions. |
| Condition evaluation | Client-side for show/hide, server-side for validation | Responsive UI without round-trips. Server validates on submit as the authoritative check. |
| Auth fields | Separate from command fields | Shared across commands, entered once per integration setup. Stored server-side, not re-entered per command. |
| Repeatable fields | Array values + repeatable groups | Covers both simple (multi-select connector IDs) and complex (grouped field sets) cases. |

---

## Open Questions

1. **Auth storage**: Should auth field values (API keys) be stored per-workspace on the backend, so users don't re-enter them each time? This is a backend concern but affects the manifest (auth_fields may need a `stored: true` flag).

2. **Conditional dependencies across commands**: Can a field in one command depend on the result of running a different command? For now the design scopes dependencies within a single command.

3. **Custom validators**: Should the manifest support custom Python validation functions beyond regex/min/max? This adds power but also complexity. The current proposal keeps it simple.

4. **Webhook/async commands**: Some integrations trigger long-running jobs. Should the manifest define polling behavior or webhook callbacks? This may belong in command metadata rather than field definitions.
