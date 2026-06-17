"""Rewrite schedule YAML files to mint slugs for non-slug names.

Uses ``ruamel.yaml`` to preserve comments, key order, and formatting when
modifying the YAML in place.
"""

from pathlib import Path
from typing import Callable, List, Set

import click
from ruamel.yaml import YAML

from paradime.core.bolt.schedule import SCHEDULE_FILE_NAMES, SCHEDULES_DIR_NAME


def _find_yaml_files(root: Path) -> List[Path]:
    """Discover schedule YAML files from ``.bolt/`` and/or the flat file."""
    files: List[Path] = []

    bolt_dir = root / SCHEDULES_DIR_NAME
    if bolt_dir.is_dir():
        files.extend(
            sorted(
                p for p in bolt_dir.rglob("*") if p.is_file() and p.suffix in (".yaml", ".yml")
            )
        )

    for name in SCHEDULE_FILE_NAMES:
        flat = root / name
        if flat.is_file():
            files.append(flat)

    return files


def mint_slugs_in_yaml_files(
    *,
    mint_fn: Callable[[List[str]], List[str]],
    root: Path,
    existing_names: Set[str] | None = None,
) -> int:
    """Walk schedule YAML files, mint slugs for non-slug names, rewrite in place.

    Two-pass approach:
    1. Collect all schedule names across all files. For names that are not valid
       slugs **and not already deployed** (i.e. not in ``existing_names``), call
       the backend to mint slugs. Names that already exist in the backend are
       grandfathered and left unchanged.
    2. Rewrite all files: update ``name`` fields, set ``display_name``, and fix
       cross-references in ``deferred_schedule``, ``turbo_ci``, and
       ``schedule_trigger`` sections.

    Args:
        mint_fn: Callable that takes a list of display names and returns slugs.
                 Typically ``client.bolt.create_schedule_slugs``.
        root: Project root directory containing ``.bolt/`` or ``paradime_schedules.yml``.
        existing_names: Schedule names already deployed in the workspace. These
                        are grandfathered and will not be rewritten, even if they
                        are not valid slugs.

    Returns:
        Number of files modified.
    """
    yaml = YAML()
    yaml.preserve_quotes = True

    files = _find_yaml_files(root)
    if not files:
        click.secho(f"No schedule YAML files found under {root}", fg="yellow")
        return 0

    click.secho(f"Scanning {len(files)} file(s):", fg="cyan")
    for f in files:
        click.secho(f"  {f}", fg="cyan")

    grandfathered = existing_names or set()

    # --- Pass 1: load all files and collect names that need slugs ---
    loaded: list[tuple[Path, dict]] = []
    names_needing_slugs: list[str] = []  # display names to mint
    # Track name → slug for all names (both existing and to-be-minted)
    name_to_slug: dict[str, str] = {}

    for filepath in files:
        doc = yaml.load(filepath)
        if not doc or "schedules" not in doc:
            click.secho(f"  Skipping {filepath} (no 'schedules' key)", fg="yellow")
            continue
        schedules = doc.get("schedules")
        if not isinstance(schedules, list):
            click.secho(f"  Skipping {filepath} ('schedules' is not a list)", fg="yellow")
            continue
        loaded.append((filepath, doc))

        for entry in schedules:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not name:
                continue
            name_str = str(name)

            if name_str in grandfathered:
                click.secho(f"  {filepath.name}: '{name_str}' exists in backend, skipping.", fg="cyan")
                display = entry.get("display_name") or name_str
                name_to_slug[str(display)] = name_str
                name_to_slug[name_str] = name_str
            else:
                display = entry.get("display_name") or name_str
                click.secho(f"  {filepath.name}: '{name_str}' not in backend, will mint slug.", fg="yellow")
                if str(display) not in names_needing_slugs:
                    names_needing_slugs.append(str(display))
                name_to_slug[name_str] = name_str  # placeholder, updated below

    if not names_needing_slugs:
        click.secho("No schedules need slug minting.", fg="green")
        return 0

    # Mint slugs from the backend
    click.secho(f"Minting {len(names_needing_slugs)} slug(s) via backend...", fg="cyan")
    minted_slugs = mint_fn(names_needing_slugs)
    for display_name, minted in zip(names_needing_slugs, minted_slugs):
        click.secho(f"  '{display_name}' -> '{minted}'", fg="green")
        name_to_slug[display_name] = minted

    # --- Pass 2: rewrite all files ---
    files_changed = 0
    for filepath, doc in loaded:
        changed = False
        schedules = doc["schedules"]

        for entry in schedules:
            if not isinstance(entry, dict):
                continue

            # Rewrite the schedule's own name (only if not grandfathered)
            name = entry.get("name")
            if name and str(name) not in grandfathered:
                old_name = str(name)
                display = entry.get("display_name") or old_name
                slug: str | None = name_to_slug.get(str(display)) or name_to_slug.get(old_name)
                if slug and slug != old_name:
                    if "display_name" not in entry:
                        entry.insert(1, "display_name", old_name)  # type: ignore[attr-defined]
                    entry["name"] = slug
                    changed = True

            # Fix deferred_schedule.deferred_schedule_name
            deferred = entry.get("deferred_schedule")
            if isinstance(deferred, dict):
                ref = deferred.get("deferred_schedule_name")
                if ref and str(ref) in name_to_slug:
                    new_ref = name_to_slug[str(ref)]
                    if new_ref != str(ref):
                        deferred["deferred_schedule_name"] = new_ref
                        changed = True

            # Fix turbo_ci.deferred_schedule_name
            turbo = entry.get("turbo_ci")
            if isinstance(turbo, dict):
                ref = turbo.get("deferred_schedule_name")
                if ref and str(ref) in name_to_slug:
                    new_ref = name_to_slug[str(ref)]
                    if new_ref != str(ref):
                        turbo["deferred_schedule_name"] = new_ref
                        changed = True

            # Fix schedule_trigger.schedule_name
            trigger = entry.get("schedule_trigger")
            if isinstance(trigger, dict):
                ref = trigger.get("schedule_name")
                if ref and str(ref) in name_to_slug:
                    new_ref = name_to_slug[str(ref)]
                    if new_ref != str(ref):
                        trigger["schedule_name"] = new_ref
                        changed = True

        if changed:
            click.secho(f"  Rewriting {filepath}", fg="green")
            yaml.dump(doc, filepath)
            files_changed += 1

    return files_changed
