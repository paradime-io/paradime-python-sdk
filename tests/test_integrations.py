import base64

import pytest

from paradime.integrations import (
    SCHEMA_VERSION,
    CommandExecutionRequest,
    CommandManifest,
    CommandType,
    Condition,
    ConditionEffect,
    ConditionGroup,
    ConditionOperator,
    DropdownOption,
    DynamicOptions,
    Field,
    FieldType,
    IntegrationManifest,
    ManifestRegistry,
    RepeatableGroup,
    Validation,
    build_cli_command,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_field(**overrides):
    defaults = {
        "id": "test_field",
        "label": "Test Field",
        "type": FieldType.TEXT,
    }
    defaults.update(overrides)
    return Field(**defaults)


def _make_command(**overrides):
    defaults = {
        "id": "test_cmd",
        "name": "Test Command",
        "type": CommandType.ACTION,
        "core_function": "mod.func",
        "cli_command": "test-cmd",
    }
    defaults.update(overrides)
    return CommandManifest(**defaults)


def _make_manifest(**overrides):
    defaults = {
        "id": "test_integration",
        "name": "Test Integration",
    }
    defaults.update(overrides)
    return IntegrationManifest(**defaults)


# ---------------------------------------------------------------------------
# Schema model tests
# ---------------------------------------------------------------------------


class TestFieldTypes:
    def test_all_field_types(self):
        for ft in FieldType:
            f = _make_field(type=ft)
            assert f.type == ft
            d = f.to_dict()
            assert d["type"] == ft.value

    def test_field_defaults(self):
        f = _make_field()
        assert f.required is False
        assert f.default is None
        assert f.repeatable is False
        assert f.order == 0
        assert f.encode is None
        assert f.help_url is None

    def test_secret_field(self):
        f = _make_field(
            id="api_key",
            label="API Key",
            type=FieldType.SECRET,
            required=True,
            env_var="MY_API_KEY",
        )
        d = f.to_dict()
        assert d["type"] == "secret"
        assert d["required"] is True
        assert d["env_var"] == "MY_API_KEY"

    def test_dropdown_with_static_options(self):
        f = _make_field(
            type=FieldType.DROPDOWN,
            options=[
                DropdownOption(label="Option A", value="a"),
                DropdownOption(label="Option B", value="b"),
            ],
        )
        d = f.to_dict()
        assert d["type"] == "dropdown"
        assert len(d["options"]) == 2
        assert d["options"][0] == {"label": "Option A", "value": "a"}

    def test_field_with_encode(self):
        f = _make_field(type=FieldType.TEXTAREA, encode="base64")
        d = f.to_dict()
        assert d["encode"] == "base64"

    def test_field_with_help_url(self):
        f = _make_field(help_url="https://example.com/docs")
        d = f.to_dict()
        assert d["help_url"] == "https://example.com/docs"


class TestValidation:
    def test_validation_to_dict_only_set_fields(self):
        v = Validation(min_value=1, max_value=100)
        d = v.to_dict()
        assert d == {"min_value": 1, "max_value": 100}
        assert "min_length" not in d

    def test_validation_with_pattern(self):
        v = Validation(pattern=r"^\d+$", pattern_message="Must be numeric")
        d = v.to_dict()
        assert d["pattern"] == r"^\d+$"
        assert d["pattern_message"] == "Must be numeric"

    def test_field_with_validation(self):
        f = _make_field(
            type=FieldType.NUMBER,
            validation=Validation(min_value=1, max_value=10080),
        )
        d = f.to_dict()
        assert d["validation"] == {"min_value": 1, "max_value": 10080}


class TestConditions:
    def test_simple_condition(self):
        c = Condition(
            field="wait_for_completion",
            operator=ConditionOperator.TRUTHY,
            effect=ConditionEffect.SHOW,
        )
        d = c.to_dict()
        assert d == {
            "field": "wait_for_completion",
            "operator": "truthy",
            "effect": "show",
        }

    def test_condition_with_value(self):
        c = Condition(
            field="mode",
            operator=ConditionOperator.EQ,
            value="advanced",
            effect=ConditionEffect.SHOW,
        )
        d = c.to_dict()
        assert d["value"] == "advanced"

    def test_condition_group_all(self):
        cg = ConditionGroup(
            all=[
                Condition(
                    field="a",
                    operator=ConditionOperator.TRUTHY,
                    effect=ConditionEffect.SHOW,
                ),
                Condition(
                    field="b",
                    operator=ConditionOperator.TRUTHY,
                    effect=ConditionEffect.SHOW,
                ),
            ],
            effect=ConditionEffect.SHOW,
        )
        d = cg.to_dict()
        assert len(d["all"]) == 2
        assert "any" not in d

    def test_condition_group_any(self):
        cg = ConditionGroup(
            any=[
                Condition(
                    field="x",
                    operator=ConditionOperator.EQ,
                    value="yes",
                    effect=ConditionEffect.SHOW,
                ),
            ],
            effect=ConditionEffect.REQUIRE,
        )
        d = cg.to_dict()
        assert len(d["any"]) == 1
        assert d["effect"] == "require"

    def test_field_with_depends_on(self):
        f = _make_field(
            depends_on=Condition(
                field="toggle",
                operator=ConditionOperator.TRUTHY,
                effect=ConditionEffect.SHOW,
            )
        )
        d = f.to_dict()
        assert d["depends_on"]["field"] == "toggle"


class TestDynamicOptions:
    def test_dynamic_options_to_dict(self):
        do = DynamicOptions(
            resolver="mod.list_items",
            depends_on_fields=["api_key"],
            label_key="name",
            value_key="id",
            refresh_on=["api_key"],
        )
        d = do.to_dict()
        assert d["resolver"] == "mod.list_items"
        assert d["depends_on_fields"] == ["api_key"]
        assert d["label_key"] == "name"
        assert d["value_key"] == "id"
        assert d["refresh_on"] == ["api_key"]

    def test_field_with_dynamic_options(self):
        f = _make_field(
            type=FieldType.DROPDOWN,
            dynamic_options=DynamicOptions(
                resolver="mod.list_items",
                label_key="name",
                value_key="id",
            ),
        )
        d = f.to_dict()
        assert d["dynamic_options"]["resolver"] == "mod.list_items"


class TestRepeatableGroup:
    def test_repeatable_group_to_dict(self):
        rg = RepeatableGroup(
            id="pair",
            label="Source/Dest Pair",
            fields=[
                _make_field(id="source", label="Source"),
                _make_field(id="dest", label="Destination"),
            ],
            min_items=1,
            max_items=5,
        )
        d = rg.to_dict()
        assert d["kind"] == "group"
        assert d["id"] == "pair"
        assert len(d["fields"]) == 2
        assert d["min_items"] == 1
        assert d["max_items"] == 5


class TestCommandManifest:
    def test_command_to_dict(self):
        cmd = _make_command(
            fields=[
                _make_field(id="connector_id", label="Connector"),
                _make_field(id="force", label="Force", type=FieldType.SWITCH),
            ]
        )
        d = cmd.to_dict()
        assert d["id"] == "test_cmd"
        assert d["type"] == "action"
        assert d["core_function"] == "mod.func"
        assert len(d["fields"]) == 2
        assert d["fields"][0]["kind"] == "field"

    def test_command_with_mixed_fields_and_groups(self):
        cmd = _make_command(
            fields=[
                _make_field(id="name", label="Name"),
                RepeatableGroup(
                    id="mappings",
                    label="Mappings",
                    fields=[
                        _make_field(id="src", label="Source"),
                        _make_field(id="dst", label="Destination"),
                    ],
                ),
            ]
        )
        d = cmd.to_dict()
        assert d["fields"][0]["kind"] == "field"
        assert d["fields"][1]["kind"] == "group"


class TestIntegrationManifest:
    def test_manifest_to_dict(self):
        m = _make_manifest(
            description="Test",
            icon="test-icon",
            category="etl",
            help_url="https://docs.example.com",
            auth_fields=[
                _make_field(id="api_key", label="API Key", type=FieldType.SECRET, required=True)
            ],
            commands=[_make_command()],
        )
        d = m.to_dict()
        assert d["id"] == "test_integration"
        assert d["name"] == "Test Integration"
        assert d["help_url"] == "https://docs.example.com"
        assert d["schema_version"] == SCHEMA_VERSION
        assert len(d["auth_fields"]) == 1
        assert len(d["commands"]) == 1

    def test_default_schema_version(self):
        m = _make_manifest()
        assert m.schema_version == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


class TestManifestRegistry:
    def setup_method(self):
        self.registry = ManifestRegistry()

    def test_register_and_list(self):
        m = _make_manifest(id="a", name="A")
        self.registry.register(m)
        assert len(self.registry.list_integrations()) == 1
        assert self.registry.list_integrations()[0].id == "a"

    def test_register_overwrites(self):
        self.registry.register(_make_manifest(id="a", name="A v1"))
        self.registry.register(_make_manifest(id="a", name="A v2"))
        assert len(self.registry.list_integrations()) == 1
        assert self.registry.get_integration("a").name == "A v2"

    def test_get_integration(self):
        self.registry.register(_make_manifest(id="x", name="X"))
        m = self.registry.get_integration("x")
        assert m.name == "X"

    def test_get_integration_not_found(self):
        with pytest.raises(KeyError, match="not found"):
            self.registry.get_integration("missing")

    def test_get_command(self):
        self.registry.register(
            _make_manifest(
                id="integ",
                commands=[
                    _make_command(id="cmd1", name="Command 1"),
                    _make_command(id="cmd2", name="Command 2"),
                ],
            )
        )
        cmd = self.registry.get_command("integ", "cmd2")
        assert cmd.name == "Command 2"

    def test_get_command_not_found(self):
        self.registry.register(_make_manifest(id="integ", commands=[_make_command(id="cmd1")]))
        with pytest.raises(KeyError, match="Command 'missing' not found"):
            self.registry.get_command("integ", "missing")

    def test_to_dict(self):
        self.registry.register(_make_manifest(id="a", name="A"))
        self.registry.register(_make_manifest(id="b", name="B"))
        result = self.registry.to_dict()
        assert isinstance(result, list)
        assert len(result) == 2
        ids = {r["id"] for r in result}
        assert ids == {"a", "b"}

    def test_clear(self):
        self.registry.register(_make_manifest(id="a", name="A"))
        self.registry.clear()
        assert len(self.registry.list_integrations()) == 0

    def test_to_dict_empty(self):
        assert self.registry.to_dict() == []


# ---------------------------------------------------------------------------
# Full Fivetran-like manifest integration test
# ---------------------------------------------------------------------------


class TestFivetranExample:
    def test_fivetran_manifest_roundtrip(self):
        manifest = IntegrationManifest(
            id="fivetran",
            name="Fivetran",
            description="ELT data pipeline platform",
            icon="fivetran",
            category="etl",
            help_url="https://docs.paradime.io/integrations/fivetran",
            auth_fields=[
                Field(
                    id="api_key",
                    label="API Key",
                    type=FieldType.SECRET,
                    required=True,
                    env_var="FIVETRAN_API_KEY",
                    description="Your Fivetran API key",
                    help_url="https://fivetran.com/docs/rest-api/getting-started",
                ),
                Field(
                    id="api_secret",
                    label="API Secret",
                    type=FieldType.SECRET,
                    required=True,
                    env_var="FIVETRAN_API_SECRET",
                    description="Your Fivetran API secret",
                    help_url="https://fivetran.com/docs/rest-api/getting-started",
                ),
            ],
            commands=[
                CommandManifest(
                    id="sync",
                    name="Trigger Sync",
                    description="Trigger sync for Fivetran connectors",
                    type=CommandType.ACTION,
                    core_function="paradime.core.scripts.fivetran.trigger_fivetran_sync",
                    cli_command="fivetran-sync",
                    fields=[
                        Field(
                            id="connector_id",
                            label="Connector(s)",
                            type=FieldType.DROPDOWN,
                            required=True,
                            repeatable=True,
                            description="The connector(s) to sync",
                            dynamic_options=DynamicOptions(
                                resolver="paradime.core.scripts.fivetran.list_fivetran_connectors",
                                depends_on_fields=["api_key", "api_secret"],
                                label_key="name",
                                value_key="connector_id",
                            ),
                            order=1,
                        ),
                        Field(
                            id="force",
                            label="Force Restart",
                            type=FieldType.SWITCH,
                            default=False,
                            description="Force restart any ongoing syncs",
                            order=2,
                        ),
                        Field(
                            id="wait_for_completion",
                            label="Wait for Completion",
                            type=FieldType.SWITCH,
                            default=True,
                            description="Wait for syncs to complete",
                            order=3,
                        ),
                        Field(
                            id="timeout_minutes",
                            label="Timeout (minutes)",
                            type=FieldType.NUMBER,
                            default=1440,
                            description="Maximum wait time in minutes",
                            depends_on=Condition(
                                field="wait_for_completion",
                                operator=ConditionOperator.TRUTHY,
                                effect=ConditionEffect.SHOW,
                            ),
                            validation=Validation(min_value=1, max_value=10080),
                            order=4,
                        ),
                    ],
                ),
                CommandManifest(
                    id="list_connectors",
                    name="List Connectors",
                    description="List all Fivetran connectors with status",
                    type=CommandType.LIST,
                    core_function="paradime.core.scripts.fivetran.list_fivetran_connectors",
                    cli_command="fivetran-list-connectors",
                    fields=[
                        Field(
                            id="group_id",
                            label="Group ID",
                            type=FieldType.TEXT,
                            description="Filter connectors by group",
                            order=1,
                        ),
                    ],
                ),
            ],
        )

        # Roundtrip via to_dict
        d = manifest.to_dict()
        assert d["id"] == "fivetran"
        assert d["help_url"] == "https://docs.paradime.io/integrations/fivetran"
        assert len(d["auth_fields"]) == 2
        assert (
            d["auth_fields"][0]["help_url"] == "https://fivetran.com/docs/rest-api/getting-started"
        )
        assert len(d["commands"]) == 2

        sync_cmd = d["commands"][0]
        assert sync_cmd["id"] == "sync"
        assert sync_cmd["type"] == "action"
        assert len(sync_cmd["fields"]) == 4

        timeout_field = sync_cmd["fields"][3]
        assert timeout_field["depends_on"]["field"] == "wait_for_completion"
        assert timeout_field["depends_on"]["operator"] == "truthy"
        assert timeout_field["validation"]["min_value"] == 1

        connector_field = sync_cmd["fields"][0]
        assert (
            connector_field["dynamic_options"]["resolver"]
            == "paradime.core.scripts.fivetran.list_fivetran_connectors"
        )

        # Registry roundtrip
        reg = ManifestRegistry()
        reg.register(manifest)
        assert reg.get_integration("fivetran").name == "Fivetran"
        assert reg.get_command("fivetran", "sync").name == "Trigger Sync"
        assert reg.get_command("fivetran", "list_connectors").type == CommandType.LIST

        serialized = reg.to_dict()
        assert len(serialized) == 1
        assert serialized[0]["id"] == "fivetran"


# ---------------------------------------------------------------------------
# CLI command builder tests
# ---------------------------------------------------------------------------


class TestBuildCliCommand:
    def _fivetran_manifest(self):
        return IntegrationManifest(
            id="fivetran",
            name="Fivetran",
            auth_fields=[
                Field(
                    id="api_key",
                    label="API Key",
                    type=FieldType.SECRET,
                    required=True,
                    env_var="FIVETRAN_API_KEY",
                ),
                Field(
                    id="api_secret",
                    label="API Secret",
                    type=FieldType.SECRET,
                    required=True,
                    env_var="FIVETRAN_API_SECRET",
                ),
            ],
            commands=[
                CommandManifest(
                    id="sync",
                    name="Trigger Sync",
                    type=CommandType.ACTION,
                    core_function="paradime.core.scripts.fivetran.trigger_fivetran_sync",
                    cli_command="fivetran-sync",
                    fields=[
                        Field(
                            id="connector_id",
                            label="Connector(s)",
                            type=FieldType.DROPDOWN,
                            required=True,
                            repeatable=True,
                        ),
                        Field(
                            id="force",
                            label="Force Restart",
                            type=FieldType.SWITCH,
                            default=False,
                        ),
                        Field(
                            id="wait_for_completion",
                            label="Wait for Completion",
                            type=FieldType.SWITCH,
                            default=True,
                        ),
                        Field(
                            id="timeout_minutes",
                            label="Timeout (minutes)",
                            type=FieldType.NUMBER,
                            default=1440,
                        ),
                    ],
                ),
            ],
        )

    def test_basic_fivetran_sync(self):
        m = self._fivetran_manifest()
        result = m.build_cli_command(
            "sync",
            {
                "connector_id": ["abc123", "def456"],
                "force": True,
                "wait_for_completion": True,
                "timeout_minutes": 60,
            },
        )
        assert result == (
            "paradime run fivetran-sync"
            " --api-key $FIVETRAN_API_KEY"
            " --api-secret $FIVETRAN_API_SECRET"
            " --connector-id abc123 --connector-id def456"
            " --force"
            " --wait-for-completion"
            " --timeout-minutes 60"
        )

    def test_boolean_false_omitted(self):
        m = self._fivetran_manifest()
        result = m.build_cli_command(
            "sync",
            {
                "connector_id": ["abc123"],
                "force": False,
                "wait_for_completion": False,
                "timeout_minutes": 60,
            },
        )
        assert "--force" not in result
        assert "--wait-for-completion" not in result
        assert "--connector-id abc123" in result
        assert "--timeout-minutes 60" in result

    def test_none_values_omitted(self):
        m = self._fivetran_manifest()
        result = m.build_cli_command(
            "sync",
            {
                "connector_id": ["abc123"],
            },
        )
        assert "--force" not in result
        assert "--timeout-minutes" not in result
        assert "--connector-id abc123" in result

    def test_auth_fields_use_env_vars(self):
        m = self._fivetran_manifest()
        result = m.build_cli_command("sync", {"connector_id": ["abc123"]})
        assert "$FIVETRAN_API_KEY" in result
        assert "$FIVETRAN_API_SECRET" in result

    def test_secret_without_env_var_omitted(self):
        m = IntegrationManifest(
            id="test",
            name="Test",
            auth_fields=[
                Field(
                    id="token",
                    label="Token",
                    type=FieldType.SECRET,
                    required=True,
                    # No env_var — should be omitted from CLI
                ),
            ],
            commands=[
                _make_command(
                    cli_command="test-cmd",
                    fields=[
                        _make_field(id="name", label="Name"),
                    ],
                ),
            ],
        )
        result = m.build_cli_command("test_cmd", {"name": "hello"})
        assert "--token" not in result
        assert "--name hello" in result

    def test_values_with_spaces_are_quoted(self):
        m = IntegrationManifest(
            id="test",
            name="Test",
            commands=[
                _make_command(
                    cli_command="test-cmd",
                    fields=[
                        _make_field(id="name", label="Name"),
                    ],
                ),
            ],
        )
        result = m.build_cli_command("test_cmd", {"name": "hello world"})
        assert "'hello world'" in result

    def test_snake_case_to_kebab_case(self):
        m = IntegrationManifest(
            id="test",
            name="Test",
            commands=[
                _make_command(
                    cli_command="test-cmd",
                    fields=[
                        _make_field(id="my_long_option", label="My Option"),
                    ],
                ),
            ],
        )
        result = m.build_cli_command("test_cmd", {"my_long_option": "val"})
        assert "--my-long-option val" in result

    def test_base64_encode_in_cli(self):
        m = IntegrationManifest(
            id="test",
            name="Test",
            commands=[
                _make_command(
                    cli_command="test-cmd",
                    fields=[
                        Field(
                            id="config_json",
                            label="Config",
                            type=FieldType.TEXTAREA,
                            encode="base64",
                        ),
                    ],
                ),
            ],
        )
        raw = '{"key": "value"}'
        result = m.build_cli_command("test_cmd", {"config_json": raw})
        encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        assert encoded in result

    def test_build_cli_command_standalone_function(self):
        m = self._fivetran_manifest()
        cmd = m.get_command("sync")
        result = build_cli_command(
            m,
            cmd,
            {
                "connector_id": ["abc123"],
                "force": True,
            },
        )
        assert result.startswith("paradime run fivetran-sync")
        assert "--force" in result

    def test_command_to_dict_includes_cli_command(self):
        cmd = _make_command(cli_command="my-cmd")
        d = cmd.to_dict()
        assert d["cli_command"] == "my-cmd"


# ---------------------------------------------------------------------------
# CommandExecutionRequest tests
# ---------------------------------------------------------------------------


class TestCommandExecutionRequest:
    def test_decode_fields_base64(self):
        cmd = _make_command(
            cli_command="test-cmd",
            fields=[
                Field(
                    id="config",
                    label="Config",
                    type=FieldType.TEXTAREA,
                    encode="base64",
                ),
                Field(id="name", label="Name", type=FieldType.TEXT),
            ],
        )
        raw = '{"key": "value"}'
        encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        req = CommandExecutionRequest(fields={"config": encoded, "name": "test"})
        decoded = req.decode_fields(cmd)
        assert decoded["config"] == raw
        assert decoded["name"] == "test"

    def test_decode_fields_no_encoding(self):
        cmd = _make_command(
            cli_command="test-cmd",
            fields=[_make_field(id="name", label="Name")],
        )
        req = CommandExecutionRequest(fields={"name": "hello"})
        decoded = req.decode_fields(cmd)
        assert decoded["name"] == "hello"

    def test_decode_fields_in_repeatable_group(self):
        cmd = _make_command(
            cli_command="test-cmd",
            fields=[
                RepeatableGroup(
                    id="configs",
                    label="Configs",
                    fields=[
                        Field(
                            id="payload",
                            label="Payload",
                            type=FieldType.TEXTAREA,
                            encode="base64",
                        ),
                    ],
                ),
            ],
        )
        raw = "test data"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        req = CommandExecutionRequest(fields={"payload": encoded})
        decoded = req.decode_fields(cmd)
        assert decoded["payload"] == raw
