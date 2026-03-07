"""Tests for the SDK UI Manifest schema, registry, CLI builder, and CLI expression generator."""

import os
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from paradime.integrations import ManifestRegistry
from paradime.integrations._base import (
    CommandManifest,
    CommandType,
    Condition,
    ConditionEffect,
    ConditionOperator,
    DropdownOption,
    DynamicOptions,
    Field,
    FieldType,
    IntegrationManifest,
    RepeatableGroup,
    Validation,
)
from paradime.integrations.cli_builder import build_click_command, build_integration_commands
from paradime.integrations.cli_expression import generate_cli_expression

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_field():
    return Field(
        id="connector_id",
        label="Connector ID",
        description="The connector to sync",
        type=FieldType.DROPDOWN,
        required=True,
        repeatable=True,
        order=1,
        dynamic_options=DynamicOptions(
            resolver="paradime.core.scripts.fivetran.list_fivetran_connectors",
            depends_on_fields=["api_key", "api_secret"],
            label_key="name",
            value_key="connector_id",
        ),
    )


@pytest.fixture
def sample_auth_fields():
    return [
        Field(
            id="api_key",
            label="API Key",
            type=FieldType.SECRET,
            required=True,
            env_var="FIVETRAN_API_KEY",
            description="Fivetran API key",
        ),
        Field(
            id="api_secret",
            label="API Secret",
            type=FieldType.SECRET,
            required=True,
            env_var="FIVETRAN_API_SECRET",
            description="Fivetran API secret",
        ),
    ]


@pytest.fixture
def sample_command(sample_field):
    return CommandManifest(
        id="sync",
        name="Trigger Sync",
        description="Trigger sync for Fivetran connectors",
        type=CommandType.ACTION,
        core_function="paradime.core.scripts.fivetran.trigger_fivetran_sync",
        fields=[
            sample_field,
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
    )


@pytest.fixture
def sample_manifest(sample_auth_fields, sample_command):
    return IntegrationManifest(
        id="fivetran",
        name="Fivetran",
        description="ELT data pipeline platform",
        icon="fivetran",
        category="etl",
        help_url="https://docs.paradime.io/integrations/fivetran",
        auth_fields=sample_auth_fields,
        commands=[
            sample_command,
            CommandManifest(
                id="list_connectors",
                name="List Connectors",
                description="List all Fivetran connectors",
                type=CommandType.LIST,
                core_function="paradime.core.scripts.fivetran.list_fivetran_connectors",
                fields=[
                    Field(
                        id="group_id",
                        label="Group ID",
                        type=FieldType.TEXT,
                        required=False,
                        description="Filter connectors by group",
                        order=1,
                    ),
                ],
            ),
        ],
    )


@pytest.fixture
def clean_registry():
    return ManifestRegistry()


# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------


class TestSchemaValidation:
    def test_create_field_minimal(self):
        field = Field(id="test", label="Test")
        assert field.id == "test"
        assert field.type == FieldType.TEXT
        assert field.required is False
        assert field.repeatable is False
        assert field.order == 0

    def test_create_field_all_properties(self):
        field = Field(
            id="config",
            label="Config JSON",
            description="Configuration payload",
            type=FieldType.TEXTAREA,
            required=True,
            encode="base64",
            help_url="https://example.com/docs",
            group="advanced",
            order=5,
        )
        assert field.encode == "base64"
        assert field.help_url == "https://example.com/docs"
        assert field.group == "advanced"

    def test_field_type_enum(self):
        for ft in ["text", "secret", "number", "dropdown", "checkbox", "switch", "textarea"]:
            field = Field(id="test", label="Test", type=FieldType(ft))
            assert field.type.value == ft

    def test_validation_rules(self):
        v = Validation(
            min_value=1, max_value=100, pattern=r"^\d+$", pattern_message="Must be numeric"
        )
        assert v.min_value == 1
        assert v.max_value == 100

    def test_condition_simple(self):
        c = Condition(
            field="wait_for_completion",
            operator=ConditionOperator.TRUTHY,
            effect=ConditionEffect.SHOW,
        )
        assert c.field == "wait_for_completion"
        assert c.operator == ConditionOperator.TRUTHY

    def test_condition_compound(self):
        c = Condition(
            all=[
                Condition(field="a", operator=ConditionOperator.TRUTHY),
                Condition(field="b", operator=ConditionOperator.EQ, value="yes"),
            ],
            effect=ConditionEffect.SHOW,
        )
        assert len(c.all) == 2

    def test_dynamic_options(self):
        do = DynamicOptions(
            resolver="some.module.func",
            depends_on_fields=["api_key"],
            label_key="name",
            value_key="id",
        )
        assert do.resolver == "some.module.func"

    def test_dropdown_static_options(self):
        field = Field(
            id="region",
            label="Region",
            type=FieldType.DROPDOWN,
            static_options=[
                DropdownOption(label="US East", value="us-east-1"),
                DropdownOption(label="EU West", value="eu-west-1"),
            ],
        )
        assert len(field.static_options) == 2

    def test_repeatable_group(self):
        rg = RepeatableGroup(
            id="sync_pair",
            label="Sync Pair",
            min_items=1,
            max_items=5,
            fields=[
                Field(id="source", label="Source"),
                Field(id="dest", label="Destination"),
            ],
        )
        assert len(rg.fields) == 2
        assert rg.max_items == 5

    def test_command_manifest(self, sample_command):
        assert sample_command.id == "sync"
        assert sample_command.type == CommandType.ACTION
        assert len(sample_command.fields) == 4

    def test_integration_manifest(self, sample_manifest):
        assert sample_manifest.id == "fivetran"
        assert len(sample_manifest.auth_fields) == 2
        assert len(sample_manifest.commands) == 2
        assert sample_manifest.schema_version == "1.0.0"

    def test_model_dump(self, sample_manifest):
        data = sample_manifest.model_dump(mode="json")
        assert isinstance(data, dict)
        assert data["id"] == "fivetran"
        assert isinstance(data["auth_fields"], list)
        assert isinstance(data["commands"], list)


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_register_and_list(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        integrations = clean_registry.list_integrations()
        assert len(integrations) == 1
        assert integrations[0].id == "fivetran"

    def test_register_overwrites(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        updated = sample_manifest.model_copy(update={"name": "Fivetran Updated"})
        clean_registry.register(updated)
        assert len(clean_registry.list_integrations()) == 1
        assert clean_registry.get_integration("fivetran").name == "Fivetran Updated"

    def test_get_integration(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        m = clean_registry.get_integration("fivetran")
        assert m.id == "fivetran"

    def test_get_integration_not_found(self, clean_registry):
        with pytest.raises(KeyError, match="not found"):
            clean_registry.get_integration("nonexistent")

    def test_get_command(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        cmd = clean_registry.get_command("fivetran", "sync")
        assert cmd.id == "sync"

    def test_get_command_not_found(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        with pytest.raises(KeyError, match="not found"):
            clean_registry.get_command("fivetran", "nonexistent")

    def test_to_dict(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        data = clean_registry.to_dict()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == "fivetran"
        # Verify JSON-serializable (no pydantic objects)
        import json

        json.dumps(data)

    def test_clear(self, clean_registry, sample_manifest):
        clean_registry.register(sample_manifest)
        clean_registry.clear()
        assert len(clean_registry.list_integrations()) == 0


# ---------------------------------------------------------------------------
# CLI Builder tests
# ---------------------------------------------------------------------------


class TestCLIBuilder:
    def test_build_command_has_correct_name(self, sample_command, sample_auth_fields):
        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        assert cmd.name == "fivetran-sync"

    def test_build_command_has_options(self, sample_command, sample_auth_fields):
        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        param_names = [p.name for p in cmd.params]
        assert "connector_id" in param_names
        assert "force" in param_names
        assert "wait_for_completion" in param_names
        assert "timeout_minutes" in param_names

    def test_build_command_no_auth_options(self, sample_command, sample_auth_fields):
        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        param_names = [p.name for p in cmd.params]
        assert "api_key" not in param_names
        assert "api_secret" not in param_names

    def test_build_command_flag_options(self, sample_command, sample_auth_fields):
        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        force_param = next(p for p in cmd.params if p.name == "force")
        assert force_param.is_flag

    def test_build_command_multiple_option(self, sample_command, sample_auth_fields):
        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        connector_param = next(p for p in cmd.params if p.name == "connector_id")
        assert connector_param.multiple

    def test_build_command_missing_env_var(self, sample_command, sample_auth_fields):
        """Test that invoking the command without env vars raises an error."""
        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        runner = CliRunner()
        result = runner.invoke(cmd, ["--connector-id", "abc123"])
        assert result.exit_code != 0
        assert "Missing required environment variables" in result.output

    @patch.dict(os.environ, {"FIVETRAN_API_KEY": "test-key", "FIVETRAN_API_SECRET": "test-secret"})
    @patch("paradime.integrations.cli_builder._import_function")
    def test_build_command_invocation(self, mock_import, sample_command, sample_auth_fields):
        mock_fn = MagicMock(return_value=["SUCCESS"])
        mock_import.return_value = mock_fn

        cmd = build_click_command(sample_command, sample_auth_fields, "fivetran")
        runner = CliRunner()
        result = runner.invoke(cmd, ["--connector-id", "abc123", "--force"])
        assert result.exit_code == 0
        mock_fn.assert_called_once()
        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["api_key"] == "test-key"
        assert call_kwargs["api_secret"] == "test-secret"
        assert call_kwargs["connector_id"] == ["abc123"]
        assert call_kwargs["force"] is True

    def test_build_integration_commands(self, sample_manifest):
        commands = build_integration_commands(sample_manifest)
        assert len(commands) == 2
        names = [c.name for c in commands]
        assert "fivetran-sync" in names
        assert "fivetran-list-connectors" in names

    def test_build_command_with_static_dropdown(self, sample_auth_fields):
        cmd_manifest = CommandManifest(
            id="test",
            name="Test",
            core_function="some.module.func",
            fields=[
                Field(
                    id="region",
                    label="Region",
                    type=FieldType.DROPDOWN,
                    required=True,
                    static_options=[
                        DropdownOption(label="US", value="us"),
                        DropdownOption(label="EU", value="eu"),
                    ],
                ),
            ],
        )
        cmd = build_click_command(cmd_manifest, sample_auth_fields, "test")
        region_param = next(p for p in cmd.params if p.name == "region")
        assert isinstance(region_param.type, click.Choice)


# ---------------------------------------------------------------------------
# CLI Expression Generator tests
# ---------------------------------------------------------------------------


class TestCLIExpression:
    def test_basic_expression(self, sample_manifest):
        expr = generate_cli_expression(
            sample_manifest,
            "sync",
            {
                "connector_id": ["abc123"],
                "force": True,
                "wait_for_completion": True,
                "timeout_minutes": 60,
            },
        )
        assert "paradime run fivetran-sync" in expr
        assert "--connector-id abc123" in expr
        assert "--force" in expr
        assert "--timeout-minutes 60" in expr

    def test_repeatable_fields(self, sample_manifest):
        expr = generate_cli_expression(
            sample_manifest,
            "sync",
            {"connector_id": ["abc", "def", "ghi"]},
        )
        assert expr.count("--connector-id") == 3

    def test_boolean_false(self, sample_manifest):
        expr = generate_cli_expression(
            sample_manifest,
            "sync",
            {"connector_id": ["abc"], "force": False},
        )
        assert "--no-force" in expr

    def test_no_auth_in_expression(self, sample_manifest):
        expr = generate_cli_expression(
            sample_manifest,
            "sync",
            {"connector_id": ["abc"], "api_key": "secret", "api_secret": "secret2"},
        )
        # Auth fields are not in the command fields, so they should not appear
        assert "--api-key" not in expr
        assert "--api-secret" not in expr

    def test_command_not_found(self, sample_manifest):
        with pytest.raises(KeyError, match="not found"):
            generate_cli_expression(sample_manifest, "nonexistent", {})

    def test_text_field(self, sample_manifest):
        expr = generate_cli_expression(
            sample_manifest,
            "list_connectors",
            {"group_id": "my-group"},
        )
        assert "paradime run fivetran-list-connectors" in expr
        assert "--group-id my-group" in expr

    def test_value_with_spaces(self, sample_manifest):
        expr = generate_cli_expression(
            sample_manifest,
            "list_connectors",
            {"group_id": "my group"},
        )
        assert "'my group'" in expr
