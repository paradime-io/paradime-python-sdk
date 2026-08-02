from pathlib import Path
from typing import Any, Dict, List

import pytest

from paradime.core.bolt.schedule import CommandSetting, ParadimeSchedule, is_valid_schedule_at_path
from paradime.core.bolt.yaml_rewriter import mint_slugs_in_yaml_files


def _schedule(commands: List[Any], **overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "name": "nightly-run",
        "schedule": "0 0 * * *",
        "environment": "production",
        "commands": commands,
    }
    base.update(overrides)
    return base


def test_bare_string_commands_unchanged() -> None:
    schedule = ParadimeSchedule.parse_obj(_schedule(["dbt seed", "dbt run"]))
    assert schedule.commands == ["dbt seed", "dbt run"]
    assert schedule.command_settings is None
    assert schedule.continue_on_error is False


def test_mixed_commands_normalized() -> None:
    schedule = ParadimeSchedule.parse_obj(
        _schedule(
            [
                "dbt seed",
                {"command": "dbt run", "continue_on_error": True},
                "dbt test",
            ]
        )
    )
    assert schedule.commands == ["dbt seed", "dbt run", "dbt test"]
    assert schedule.command_settings is not None
    assert [setting.continue_on_error for setting in schedule.command_settings] == [
        None,
        True,
        None,
    ]


def test_per_command_override_false() -> None:
    schedule = ParadimeSchedule.parse_obj(
        _schedule(
            [
                {"command": "dbt run", "continue_on_error": False},
                {"command": "dbt test"},
            ],
            continue_on_error=True,
        )
    )
    assert schedule.continue_on_error is True
    assert schedule.commands == ["dbt run", "dbt test"]
    assert schedule.command_settings is not None
    assert [setting.continue_on_error for setting in schedule.command_settings] == [False, None]


def test_object_command_unknown_key_rejected() -> None:
    with pytest.raises(Exception, match="continue_on_error"):
        ParadimeSchedule.parse_obj(_schedule([{"command": "dbt run", "continue_on_err": True}]))


def test_object_command_missing_command_key_rejected() -> None:
    with pytest.raises(Exception, match="'command'"):
        ParadimeSchedule.parse_obj(_schedule([{"continue_on_error": True}]))


def test_schedule_level_continue_on_error() -> None:
    schedule = ParadimeSchedule.parse_obj(_schedule(["dbt build"], continue_on_error=True))
    assert schedule.continue_on_error is True
    assert schedule.commands == ["dbt build"]
    assert schedule.command_settings is None


def test_command_settings_passed_directly() -> None:
    schedule = ParadimeSchedule.parse_obj(
        _schedule(
            ["dbt run", "dbt test"],
            command_settings=[{"continue_on_error": True}, {"continue_on_error": None}],
        )
    )
    assert schedule.command_settings == [
        CommandSetting(continue_on_error=True),
        CommandSetting(continue_on_error=None),
    ]


def test_command_settings_length_mismatch_rejected() -> None:
    with pytest.raises(Exception, match="length"):
        ParadimeSchedule.parse_obj(
            _schedule(
                ["dbt run", "dbt test"],
                command_settings=[{"continue_on_error": True}],
            )
        )


def test_object_commands_with_direct_command_settings_rejected() -> None:
    with pytest.raises(Exception, match="not both"):
        ParadimeSchedule.parse_obj(
            _schedule(
                [{"command": "dbt run", "continue_on_error": True}],
                command_settings=[{"continue_on_error": False}],
            )
        )


def test_json_round_trip_keeps_commands_as_strings() -> None:
    schedule = ParadimeSchedule.parse_obj(
        _schedule(
            ["dbt seed", {"command": "dbt run", "continue_on_error": True}],
            continue_on_error=True,
        )
    )
    reparsed = ParadimeSchedule.parse_obj(schedule.dict())
    assert reparsed.commands == ["dbt seed", "dbt run"]
    assert reparsed.command_settings == schedule.command_settings
    assert reparsed.continue_on_error is True


def test_is_valid_schedule_at_path_accepts_object_form(tmp_path: Path) -> None:
    yaml_file = tmp_path / "paradime_schedules.yml"
    yaml_file.write_text(
        """
schedules:
  - name: nightly-run
    schedule: "0 0 * * *"
    environment: production
    continue_on_error: true
    commands:
      - dbt seed
      - command: "dbt run"
        continue_on_error: false
      - dbt test
"""
    )
    assert is_valid_schedule_at_path(yaml_file) is None


def test_is_valid_schedule_at_path_rejects_unknown_command_key(tmp_path: Path) -> None:
    yaml_file = tmp_path / "paradime_schedules.yml"
    yaml_file.write_text(
        """
schedules:
  - name: nightly-run
    schedule: "0 0 * * *"
    environment: production
    commands:
      - command: "dbt run"
        continue_on_failure: true
"""
    )
    error = is_valid_schedule_at_path(yaml_file)
    assert error is not None
    assert "continue_on_failure" in error


def test_mint_slugs_round_trips_object_form_commands(tmp_path: Path) -> None:
    yaml_file = tmp_path / "paradime_schedules.yml"
    yaml_file.write_text(
        """schedules:
  - name: My Nightly Run
    schedule: "0 0 * * *"
    environment: production
    continue_on_error: true
    commands:
      - dbt seed
      - command: "dbt run"
        continue_on_error: false
      - dbt test
"""
    )

    changed = mint_slugs_in_yaml_files(
        mint_fn=lambda names: [f"minted-{i}" for i, _ in enumerate(names)],
        root=tmp_path,
    )
    assert changed == 1

    rewritten = yaml_file.read_text()
    assert "slug: minted-0" in rewritten
    # Object-form command entries must survive the rewrite unmangled.
    assert 'command: "dbt run"' in rewritten
    assert "continue_on_error: false" in rewritten
    assert is_valid_schedule_at_path(yaml_file) is None
