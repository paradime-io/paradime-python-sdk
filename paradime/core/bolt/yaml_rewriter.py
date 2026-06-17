"""Rewrite schedule YAML files to mint slugs for non-slug names.

Uses ``ruamel.yaml`` to preserve comments, key order, and formatting when
modifying the YAML in place.
"""

from pathlib import Path
from typing import Callable, List, Set

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
        return 0

    grandfathered = existing_names or set()

    # --- Pass 1: load all files and collect names that need slugs ---
    loaded: list[tuple[Path, dict]] = []
    names_needing_slugs: list[str] = []  # display names to mint
    # Track name → slug for all names (both existing and to-be-minted)
    name_to_slug: dict[str, str] = {}

    for filepath in files:
        doc = yaml.load(filepath)
        if not doc or "schedules" not in doc:
            continue
        schedules = doc.get("schedules")
        if not isinstance(schedules, list):
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
                # Exists in the backend — leave it alone
                display = entry.get("display_name") or name_str
                name_to_slug[str(display)] = name_str
                name_to_slug[name_str] = name_str
            else:
                # New schedule, not a slug — needs minting
                display = entry.get("display_name") or name_str
                if str(display) not in names_needing_slugs:
                    names_needing_slugs.append(str(display))
                name_to_slug[name_str] = name_str  # placeholder, updated below

    if not names_needing_slugs:
        return 0

    # Mint slugs from the backend
    minted_slugs = mint_fn(names_needing_slugs)
    for display_name, minted in zip(names_needing_slugs, minted_slugs):
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
            yaml.dump(doc, filepath)
            files_changed += 1

    return files_changed
