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
            sorted(p for p in bolt_dir.rglob("*") if p.is_file() and p.suffix in (".yaml", ".yml"))
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

    v3 format: the human-readable ``name`` is left in place and the minted slug
    is inserted as ``slug``. Cross-references use ``deferred_schedule_slug`` and
    ``schedule_slug`` fields.

    v2 → v3 auto-migration: when a schedule already has ``name`` (a slug) and
    ``display_name`` (the human label), it is converted to v3 by swapping them.

    Two-pass approach:
    1. Collect all schedule names across all files. For names that are not valid
       slugs **and not already deployed** (i.e. not in ``existing_names``), call
       the backend to mint slugs. Names that already exist in the backend are
       grandfathered and left unchanged. v2 schedules with ``display_name`` are
       marked for auto-migration (no minting needed).
    2. Rewrite all files: insert ``slug`` fields, migrate v2 → v3, and fix
       cross-references.

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
    # Track display_name/old_name → slug for all names
    name_to_slug: dict[str, str] = {}
    has_v2_to_migrate = False

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

            existing_slug = entry.get("slug")
            name = entry.get("name")
            if not name:
                continue
            name_str = str(name)
            display_name = entry.get("display_name")

            if existing_slug:
                # Already v3: map the display name to the existing slug
                name_to_slug[name_str] = str(existing_slug)
            elif display_name:
                # v2 format: name is the slug, display_name is the label.
                # Auto-migrate to v3 (no minting needed).
                name_to_slug[name_str] = name_str
                name_to_slug[str(display_name)] = name_str
                has_v2_to_migrate = True
            elif name_str in grandfathered:
                name_to_slug[name_str] = name_str
            else:
                if name_str not in names_needing_slugs:
                    names_needing_slugs.append(name_str)
                name_to_slug[name_str] = name_str  # placeholder, updated below

    if not names_needing_slugs and not has_v2_to_migrate:
        return 0

    # Mint slugs from the backend (only for genuinely new names)
    if names_needing_slugs:
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

            name = entry.get("name")
            existing_slug = entry.get("slug")
            display_name = entry.get("display_name")

            if existing_slug:
                # Already v3, nothing to do for this schedule's own fields
                pass
            elif name and display_name:
                # v2 → v3 migration: name is the slug, display_name is the label.
                # Swap: name ← display_name, insert slug ← old name, remove display_name.
                old_slug = str(name)
                entry["name"] = str(display_name)
                # Insert slug right after name
                entry.insert(1, "slug", old_slug)  # type: ignore[attr-defined]
                del entry["display_name"]
                changed = True
            elif name and str(name) not in grandfathered:
                # New schedule needing a minted slug
                name_str = str(name)
                slug: str | None = name_to_slug.get(name_str)
                if slug and slug != name_str:
                    entry.insert(1, "slug", slug)  # type: ignore[attr-defined]
                    changed = True

            # Fix deferred_schedule cross-reference (use deferred_schedule_slug)
            deferred = entry.get("deferred_schedule")
            if isinstance(deferred, dict):
                ref = deferred.get("deferred_schedule_name")
                if ref and str(ref) in name_to_slug:
                    new_ref = name_to_slug[str(ref)]
                    if new_ref != str(ref):
                        deferred["deferred_schedule_slug"] = new_ref
                        changed = True

            # Fix turbo_ci cross-reference (use deferred_schedule_slug)
            turbo = entry.get("turbo_ci")
            if isinstance(turbo, dict):
                ref = turbo.get("deferred_schedule_name")
                if ref and str(ref) in name_to_slug:
                    new_ref = name_to_slug[str(ref)]
                    if new_ref != str(ref):
                        turbo["deferred_schedule_slug"] = new_ref
                        changed = True

            # Fix schedule_trigger cross-reference (use schedule_slug)
            trigger = entry.get("schedule_trigger")
            if isinstance(trigger, dict):
                ref = trigger.get("schedule_name")
                if ref and str(ref) in name_to_slug:
                    new_ref = name_to_slug[str(ref)]
                    if new_ref != str(ref):
                        trigger["schedule_slug"] = new_ref
                        changed = True

        if changed:
            yaml.dump(doc, filepath)
            files_changed += 1

    return files_changed
