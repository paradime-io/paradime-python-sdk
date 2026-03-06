"""
SDK UI Manifest Registry.

Provides a registry for integration manifests that can be populated
by the backend at startup and queried for manifest data.
"""

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


class ManifestRegistry:
    """Registry for integration manifests."""

    def __init__(self) -> None:
        self._integrations: dict[str, IntegrationManifest] = {}

    def register(self, manifest: IntegrationManifest) -> None:
        """Register an integration manifest."""
        self._integrations[manifest.id] = manifest

    def list_integrations(self) -> list[IntegrationManifest]:
        """Return all registered integration manifests."""
        return list(self._integrations.values())

    def get_integration(self, id: str) -> IntegrationManifest:
        """Get a specific integration manifest by ID. Raises KeyError if not found."""
        if id not in self._integrations:
            raise KeyError(f"Integration '{id}' not found in registry")
        return self._integrations[id]

    def get_command(self, integration_id: str, command_id: str) -> CommandManifest:
        """Get a specific command manifest. Raises KeyError if not found."""
        integration = self.get_integration(integration_id)
        for command in integration.commands:
            if command.id == command_id:
                return command
        raise KeyError(f"Command '{command_id}' not found in integration '{integration_id}'")

    def to_dict(self) -> list[dict]:
        """Export all manifests as JSON-serializable dicts."""
        return [m.model_dump(mode="json") for m in self._integrations.values()]

    def clear(self) -> None:
        """Clear all registered manifests."""
        self._integrations.clear()


# Module-level singleton
registry = ManifestRegistry()

__all__ = [
    "CommandManifest",
    "CommandType",
    "Condition",
    "ConditionEffect",
    "ConditionOperator",
    "DropdownOption",
    "DynamicOptions",
    "Field",
    "FieldType",
    "IntegrationManifest",
    "ManifestRegistry",
    "RepeatableGroup",
    "Validation",
    "registry",
]
