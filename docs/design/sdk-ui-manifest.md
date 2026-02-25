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
- Enough information for the CLI, the backend API, and the frontend to all work from the same definition

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     SDK (this repo)                         │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  Integration  │  │  Integration │  │  Integration     │  │
│  │  Manifest     │──│  Core Funcs  │──│  CLI (generated) │  │
│  │  (source of   │  │  (execute)   │  │  (auto-derived)  │  │
│  │   truth)      │  │              │  │                  │  │
│  └──────┬───────┘  └──────────────┘  └──────────────────┘  │
│         │                                                   │
│  ┌──────▼───────┐                                           │
│  │  Registry     │  ← collects all integration manifests    │
│  └──────┬───────┘                                           │
└─────────┼───────────────────────────────────────────────────┘
          │
          │  SDK is a dependency
          ▼
┌─────────────────────────────────────────────────────────────┐
│                  paradime-backend                            │
│                                                             │
│  ┌──────────────────┐     ┌─────────────────────────────┐  │
│  │  /integrations    │     │  Dynamic Field Resolver      │  │
│  │  API endpoint     │     │  - calls list_connectors()   │  │
│  │                   │     │  - calls list_workbooks()    │  │
│  │  1. reads registry│     │  - populates dropdown opts   │  │
│  │  2. resolves      │────▶│  - caches where appropriate  │  │
│  │     dynamic fields│     │                              │  │
│  │  3. returns JSON  │     └─────────────────────────────┘  │
│  └────────┬─────────┘                                       │
└───────────┼─────────────────────────────────────────────────┘
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
| `textarea` | N/A (UI only) | `<textarea>` | For longer text like JSON payloads |

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

## Part 2: Where Manifests Live in the SDK

### Proposed Directory Structure

```
paradime/
├── integrations/              # NEW - unified integration definitions
│   ├── __init__.py            # Registry: collects all manifests
│   ├── _base.py               # Base classes / Pydantic models for manifest schema
│   ├── fivetran/
│   │   ├── __init__.py        # Exports the manifest
│   │   ├── manifest.py        # The manifest definition (source of truth)
│   │   ├── commands.py        # Core functions (moved from core/scripts/fivetran.py)
│   │   └── cli.py             # CLI commands (auto-generated or thin wrapper)
│   ├── tableau/
│   │   ├── __init__.py
│   │   ├── manifest.py
│   │   ├── commands.py
│   │   └── cli.py
│   ├── hightouch/
│   │   └── ...
│   └── ...
```

### Why Co-locate

- The manifest, the function it calls, and the CLI command it generates are all in the same directory. When you add a new integration, everything is in one place.
- The manifest **is** the documentation for what the integration supports.
- No drift between CLI options and UI fields because both are derived from the manifest.

### Migration Path

This is a structural change but not a breaking one:
1. Keep the existing `paradime/cli/integrations/` and `paradime/core/scripts/` working as-is.
2. Build the new `paradime/integrations/` structure alongside them.
3. Gradually migrate integrations one by one to the new structure.
4. Once fully migrated, the old paths become thin re-exports for backward compatibility, then eventually get removed.

---

## Part 3: The Registry

The registry is the entry point that the backend uses to discover all available integrations and their manifests.

### Responsibilities

1. **Discovery**: Collect all integration manifests at import time
2. **Lookup**: Get a specific integration or command manifest by ID
3. **Serialization**: Export all manifests as JSON for the backend API to return

### API Surface

```
registry.list_integrations() -> list[IntegrationManifest]
registry.get_integration(id: str) -> IntegrationManifest
registry.get_command(integration_id: str, command_id: str) -> CommandManifest
registry.to_json() -> str   # full JSON export of all manifests
```

### Registration Pattern

Each integration registers itself with the registry on import:

```
# paradime/integrations/fivetran/__init__.py
from paradime.integrations import registry
from .manifest import manifest

registry.register(manifest)
```

---

## Part 4: Backend Integration (paradime-backend)

The backend consumes the SDK as a dependency and exposes integration manifests to the frontend via API.

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
   │  GET /integrations        │                              │
   │ ─────────────────────────>│  registry.list_integrations()│
   │                           │─────────────────────────────>│
   │   { integrations: [...] } │                              │
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

- **Integration manifests**: Cache indefinitely (only change on SDK version bump)
- **Resolved dynamic options**: Cache with a short TTL (e.g., 60s) keyed by `(resolver, context_hash)`
- Cache invalidation on explicit user request ("refresh" button in UI)

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

To illustrate how an existing integration would look under this system:

```
Integration:
  id: fivetran
  name: Fivetran
  description: ELT data pipeline platform
  icon: fivetran
  category: etl

  auth_fields:
    - id: api_key
      label: API Key
      type: secret
      required: true
      env_var: FIVETRAN_API_KEY
      description: "Your Fivetran API key from account settings"

    - id: api_secret
      label: API Secret
      type: secret
      required: true
      env_var: FIVETRAN_API_SECRET
      description: "Your Fivetran API secret from account settings"

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

### SDK Version Coupling

The backend pins a specific SDK version. When the SDK is updated:
1. New integrations automatically appear (via registry)
2. Changed manifests update the UI automatically
3. No frontend deploy needed for new integrations (the renderer is generic)

---

## Part 10: Implementation Phases

### Phase 1: Foundation
- Define the manifest Pydantic models in `paradime/integrations/_base.py`
- Build the registry in `paradime/integrations/__init__.py`
- Add a `to_dict()` / `to_json()` serialization method on all manifest models
- Write the manifest for 1-2 integrations (e.g., Fivetran, Hightouch) as a proof of concept

### Phase 2: CLI Generation
- Build the `build_click_command()` utility
- Verify that generated CLI commands are functionally identical to hand-written ones
- Add tests comparing generated CLI help output to existing

### Phase 3: Backend API
- Add the `/integrations` endpoint to paradime-backend
- Add the `/resolve-options` endpoint
- Add the `/execute` endpoint
- Wire up caching for resolved options

### Phase 4: Frontend Renderer
- Build the generic `ManifestFormRenderer` component
- Implement all field type renderers
- Implement conditional visibility
- Implement dynamic dropdown resolution
- Implement repeatable fields / groups

### Phase 5: Migration
- Convert remaining integrations one by one to the manifest format
- Deprecate hand-written CLI commands
- Remove old frontend hardcoded forms

---

## Part 11: Design Constraints and Decisions

| Decision | Chosen Approach | Rationale |
|---|---|---|
| Manifest format | Python (Pydantic models) | Type-safe, validated at import time, IDE autocompletion. JSON is generated from this, not the other way around. |
| Manifest location | Co-located per integration | One directory = one integration. Easy to find, easy to review. |
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
