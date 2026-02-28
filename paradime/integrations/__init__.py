from typing import Dict, List

from paradime.integrations._base import (  # noqa: F401
    SCHEMA_VERSION,
    CommandManifest,
    CommandType,
    Condition,
    ConditionEffect,
    ConditionGroup,
    ConditionOperator,
    DependsOn,
    DropdownOption,
    DynamicOptions,
    Field,
    FieldOrGroup,
    FieldType,
    IntegrationManifest,
    RepeatableGroup,
    Validation,
)


class ManifestRegistry:
    def __init__(self) -> None:
        self._integrations: Dict[str, IntegrationManifest] = {}

    def register(self, manifest: IntegrationManifest) -> None:
        self._integrations[manifest.id] = manifest

    def list_integrations(self) -> List[IntegrationManifest]:
        return list(self._integrations.values())

    def get_integration(self, integration_id: str) -> IntegrationManifest:
        if integration_id not in self._integrations:
            raise KeyError(f"Integration '{integration_id}' not found in registry")
        return self._integrations[integration_id]

    def get_command(self, integration_id: str, command_id: str) -> CommandManifest:
        integration = self.get_integration(integration_id)
        for command in integration.commands:
            if command.id == command_id:
                return command
        raise KeyError(
            f"Command '{command_id}' not found in integration '{integration_id}'"
        )

    def to_dict(self) -> List[dict]:
        return [m.to_dict() for m in self._integrations.values()]

    def clear(self) -> None:
        self._integrations.clear()


registry = ManifestRegistry()
