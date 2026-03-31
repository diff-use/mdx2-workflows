"""
Prefect pipeline for the single-crystal workflow.

Runs the full single-crystal workflow matching the reference script:
DIALS: import (data) → find_spots → index → refine → import (background);
mdx2: import_data refined.expt (--datastore) → import_geometry → find_peaks → mask_peaks →
import_data background (--datastore_bg) → bin_image_series → integrate → correct →
scale [--mca2020] → merge → map slice → map offslice.

If deployment.json exists in the current working directory (or in working_dir
when set), its keys are used to fill in the flow parameters. Use --file to
specify a config file by name. Use raw_data_dir to read
inputs from another directory (e.g. raw_data/insulin) and write all outputs to
working_dir (e.g. processed_data/insulin).

Run from the mdx2-dev env (e.g. in Docker or with micromamba activate mdx2-dev):
  mdx2_workflows.pipeline --file deployment.json
  mdx2_workflows.pipeline --working_dir /path/to/processed_data
"""

import json
import logging
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from prefect import flow, task
from prefect.logging import get_run_logger

def _find_prefect_flows_dir() -> Optional[Path]:
    """Locate the directory containing prefect_flows.py.

    When running from source the ``prefect/`` directory is a sibling of the
    package.  When pip-installed inside a container the source tree may be
    volume-mounted elsewhere, so we walk up from cwd checking each ancestor.
    """
    # Direct sibling of the package (works when running from source checkout)
    source_candidate = Path(__file__).resolve().parent.parent / "prefect"
    if (source_candidate / "prefect_flows.py").is_file():
        return source_candidate.resolve()

    # Walk up from cwd: at each level check prefect/ and mdx2-workflows/prefect/
    for ancestor in [Path.cwd().resolve()] + list(Path.cwd().resolve().parents):
        for subpath in ("mdx2-workflows/prefect", "prefect"):
            candidate = ancestor / subpath
            if (candidate / "prefect_flows.py").is_file():
                return candidate.resolve()

    return None


_prefect_dir = _find_prefect_flows_dir()
if _prefect_dir and str(_prefect_dir) not in sys.path:
    sys.path.insert(0, str(_prefect_dir))

from prefect_flows import (  # noqa: E402
    directory_setup,
    populate_deployment_images,
    run_conda_command,
    run_mdx2_cli_command,
)

_log = logging.getLogger(__name__)


def _shell_join(args: List[Any]) -> str:
    """Return a shell-safe, human-readable command string."""
    return " ".join(shlex.quote(str(arg)) for arg in args)


def _announce_command(
    tool_name: str,
    command: List[Any],
    working_dir: Optional[str] = None,
    logger: Optional[Any] = None,
) -> None:
    """Print and log command line before execution."""
    cmd_str = _shell_join(command)
    cwd = working_dir or str(Path.cwd())
    message = f"[{tool_name}] {cmd_str} (cwd={cwd})"
    print(message)
    (logger or _log).info(message)


def _default_deployment_json(crystal_name: str, raw_dir: str, processed_dir: str) -> dict:
    """Return default deployment.json content for a crystal. raw_data_dir is relative to processed_data/<crystal>/.

    crystal_files/background_files are intentionally omitted here; they are populated later by
    populate_deployment_images based on discovered HDF5 masters.
    """
    return {
        "working_dir": ".",
        "raw_data_dir": f"../../{raw_dir}/{crystal_name}",
        "run_dials": True,
        "space_group": 199,
        "refined_expt": "refined.expt",
        "background_expt": "background.expt",
        "integrate_subdivide": "4 4 4",
        "count_threshold": 20,
        "sigma_cutoff": 3,
        "nproc": 1,
        "datastore": "datastore",
        "datastore_bg": "datastore_bg",
        "mca2020": False,
    }


def _populate_deployment_images(deployment_file: str) -> dict:
    """
    Populate deployment.json with paths discovered from raw HDF5 masters:
    - background_files: dataset->list mapping of *_bg_*master.h5 relative to raw_data_dir
    - crystal_files: dataset->list mapping of non-bg *_master.h5 relative to raw_data_dir

    Returns the updated config dict. Call before validation so empty values can be filled.
    """
    dep_path = Path(deployment_file).resolve()
    if not dep_path.is_file():
        raise FileNotFoundError(f"deployment.json not found: {dep_path}")

    with open(dep_path) as f:
        config = json.load(f)

    raw_data_dir = config.get("raw_data_dir")
    if not raw_data_dir:
        raise ValueError(f"raw_data_dir is missing in {dep_path}")

    raw_base = (dep_path.parent / raw_data_dir).resolve()
    if not raw_base.is_dir():
        _log.warning("raw_data_dir does not exist: %s", raw_base)
        return config

    bg_abs = sorted(set(raw_base.glob("*/*_bg_*master.h5")))
    all_master_abs = sorted(set(raw_base.glob("*/*_master.h5")))
    data_abs = [p for p in all_master_abs if "_bg_" not in p.name]

    def rel(p: Path) -> str:
        try:
            return str(p.relative_to(raw_base))
        except ValueError:
            return str(p)

    bg_files = [rel(p) for p in bg_abs]
    crystal_files = [rel(p) for p in data_abs]

    config["background_files"] = _group_files_by_dataset(bg_files)
    config["crystal_files"] = _group_files_by_dataset(crystal_files)

    with open(dep_path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")

    _log.info(
        "Updated %s (crystal_files=%s, background_files=%s)",
        dep_path,
        len(crystal_files),
        len(bg_files),
    )
    return config


CONFIG_FILENAMES = ("deployment.json",)

CONFIG_KEYS = frozenset({
    "working_dir",
    "raw_data_dir",
    "raw_dir",
    "processed_dir",
    "run_dials",
    "crystal_files",
    "background_files",
    "space_group",
    "refined_expt",
    "background_expt",
    "integrate_subdivide",
    "count_threshold",
    "sigma_cutoff",
    "nproc",
    "datastore",
    "datastore_bg",
    "mca2020",
})


def load_single_crystal_config(search_dir: Path) -> Tuple[Dict[str, Any], Optional[Path]]:
    """
    Load single-crystal pipeline parameters from search_dir.
    Looks for deployment.json.
    Returns (params_dict, path_to_file) or ({}, None) if none found.
    """
    for name in CONFIG_FILENAMES:
        path = search_dir / name
        if not path.is_file():
            continue
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        return {k: v for k, v in data.items() if k in CONFIG_KEYS}, path
    return {}, None


def load_single_crystal_config_from_file(path: Path) -> Tuple[Dict[str, Any], Path]:
    """
    Load single-crystal pipeline parameters from an explicit config file path.
    Returns (params_dict, path_to_file). Raises FileNotFoundError if path does not exist.
    """
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if k in CONFIG_KEYS}, path


def _dataset_group_key(file_path: str) -> str:
    """Infer dataset key for grouping runs (e.g. insulin_1, insulin_2)."""
    p = Path(file_path)
    if len(p.parts) >= 2:
        return p.parts[0]
    name = p.name
    if "_bg_" in name:
        return name.split("_bg_", 1)[0]
    stem = p.stem.replace("_master", "")
    parts = stem.split("_")
    if len(parts) >= 3 and parts[-1].isdigit():
        return "_".join(parts[:-1])
    return stem


def _group_files_by_dataset(files: List[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {}
    for f in files:
        grouped.setdefault(_dataset_group_key(f), []).append(f)
    return grouped


def _normalize_dataset_files(files: Any, field_name: str) -> Dict[str, List[str]]:
    """Normalize crystal/background file config into {dataset: [paths...]}."""
    if files is None:
        return {}
    if isinstance(files, str):
        return _group_files_by_dataset([files])
    if isinstance(files, list):
        return _group_files_by_dataset([str(x) for x in files if x])
    if isinstance(files, dict):
        normalized: Dict[str, List[str]] = {}
        for dataset, values in files.items():
            key = str(dataset)
            if isinstance(values, str):
                entries = [values]
            elif isinstance(values, list):
                entries = [str(x) for x in values if x]
            else:
                raise ValueError(
                    f"{field_name}[{key!r}] must be a string or list of strings, got {type(values).__name__}"
                )
            if entries:
                normalized[key] = entries
        return normalized
    raise ValueError(f"{field_name} must be a dict/list/string, got {type(files).__name__}")


STABILITY_SECONDS = 120


def _newest_mtime(directory: Path) -> Optional[float]:
    """Return the mtime of the newest file under *directory*, or None if empty."""
    newest = None
    for p in directory.rglob("*"):
        if p.is_file():
            mt = p.stat().st_mtime
            if newest is None or mt > newest:
                newest = mt
    return newest


@task(persist_result=True, name="wait-for-data-ready")
def task_wait_for_data_ready(
    data_dir: str,
    stability_seconds: float = STABILITY_SECONDS,
    poll_interval: float = 30,
) -> float:
    """Poll *data_dir* until the newest file is older than *stability_seconds*.

    This ensures data collection has finished before the pipeline proceeds.
    Returns the age (in seconds) of the newest file at the time the check
    passed.
    """
    logger = get_run_logger()
    target = Path(data_dir).resolve()
    if not target.is_dir():
        logger.warning("Data directory does not exist yet: %s — skipping wait", target)
        return 0.0

    logger.info(
        "Waiting for data in %s to stabilise (no new files for %ds)…",
        target, int(stability_seconds),
    )
    while True:
        newest = _newest_mtime(target)
        if newest is None:
            logger.info("No files found in %s — skipping wait", target)
            return 0.0
        age = time.time() - newest
        if age >= stability_seconds:
            logger.info(
                "Newest file in %s is %.0fs old (threshold %ds) — data is ready",
                target, age, int(stability_seconds),
            )
            return age
        remaining = stability_seconds - age
        logger.info(
            "Newest file in %s is %.0fs old — waiting ~%.0fs more (polling every %ds)",
            target, age, remaining, int(poll_interval),
        )
        time.sleep(min(poll_interval, remaining + 1))


# ---------------------------------------------------------------------------
# Prefect tasks — each pipeline step is a named task for Prefect UI visibility
# ---------------------------------------------------------------------------

def _run_dials_command(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    logger = get_run_logger()
    cmd = ["micromamba", "run", "-n", "mdx2-dev"] + argv
    _announce_command("DIALS", cmd, working_dir, logger)
    return run_conda_command(argv, working_dir=working_dir, log_file=log_file)


def _run_mdx2_command(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    logger = get_run_logger()
    cmd = [
        "micromamba", "run", "-n", "mdx2-dev",
        "python", "-m", f"mdx2.command_line.{command}",
    ] + args
    _announce_command("MDX2", cmd, working_dir, logger)
    return run_mdx2_cli_command(command, args, working_dir=working_dir, log_file=log_file)


@task(persist_result=True, name="dials-import")
def task_dials_import(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-find-spots")
def task_dials_find_spots(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-spot-counts-per-image")
def task_dials_spot_counts_per_image(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-index")
def task_dials_index(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-refine")
def task_dials_refine(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-import-background")
def task_dials_import_background(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-integrate")
def task_dials_integrate(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-scale")
def task_dials_scale(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-export")
def task_dials_export(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="dials-merge")
def task_dials_merge(argv: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_dials_command(argv, working_dir, log_file)


@task(persist_result=True, name="mdx2-import-data")
def task_mdx2_import_data(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-import-geometry")
def task_mdx2_import_geometry(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-find-peaks")
def task_mdx2_find_peaks(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-mask-peaks")
def task_mdx2_mask_peaks(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-bin-image-series")
def task_mdx2_bin_image_series(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-integrate")
def task_mdx2_integrate(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-correct")
def task_mdx2_correct(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-scale")
def task_mdx2_scale(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-merge")
def task_mdx2_merge(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


@task(persist_result=True, name="mdx2-map")
def task_mdx2_map(command: str, args: list, working_dir: Optional[str], log_file: Optional[str] = None):
    return _run_mdx2_command(command, args, working_dir, log_file)


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="single-crystal-workflow", log_prints=True)
def single_crystal_workflow(
    working_dir: Optional[str] = None,
    raw_data_dir: Optional[str] = None,
    config_file: Optional[str] = None,
    raw_dir: str = "raw_data",
    processed_dir: str = "processed_data",
    run_dials: bool = True,
    crystal_files: Optional[Any] = None,
    background_files: Optional[Any] = None,
    space_group: int = 199,
    refined_expt: str = "refined.expt",
    background_expt: str = "background.expt",
    integrate_subdivide: str = "2 2 2",
    count_threshold: int = 20,
    sigma_cutoff: int = 3,
    nproc: int = 1,
    datastore: str = "datastore",
    datastore_bg: str = "datastore_bg",
    mca2020: bool = False,
    batch_child: bool = False,
    skip_setup: bool = False,
) -> list:
    """
    Run the single-crystal workflow: DIALS then mdx2.

    When run_dials is True (default), runs DIALS in raw_data_dir (or working_dir):
    dials.import → find_spots → index → refine, then background import. Produces
    refined.expt and background.expt. Then runs the mdx2 chain: import_geometry →
    import_data → find_peaks → mask_peaks → (background bin) → integrate → correct →
    scale → merge → map. Set run_dials=false if refined.expt and background.expt
    already exist. Config file (deployment.json) can override all parameters.

    Set skip_setup=true to bypass directory_setup and populate_deployment_images,
    useful when re-running with a manually edited deployment.json after a crash.
    """
    logger = get_run_logger()

    # Load config: from --file path if given, else search working_dir or cwd for default names
    if config_file:
        cfg_path = Path(config_file)
        if not cfg_path.is_absolute():
            base = (Path(working_dir) if working_dir else Path.cwd()).resolve()
            cfg_path = (base / config_file).resolve()
        config, config_path = load_single_crystal_config_from_file(cfg_path)
        search_dir = config_path.parent
    else:
        search_dir = Path(working_dir) if working_dir else Path.cwd()
        config, config_path = load_single_crystal_config(search_dir)
    if config:
        logger.info("Loaded parameters from %s", config_path)
        working_dir = config.get("working_dir", working_dir)
        raw_data_dir = config.get("raw_data_dir", raw_data_dir)
        raw_dir = config.get("raw_dir", raw_dir)
        processed_dir = config.get("processed_dir", processed_dir)
        run_dials = config.get("run_dials", run_dials)
        crystal_files = config.get("crystal_files", crystal_files)
        background_files = config.get("background_files", background_files)
        space_group = config.get("space_group", space_group)
        refined_expt = config.get("refined_expt", refined_expt)
        background_expt = config.get("background_expt", background_expt)
        integrate_subdivide = config.get("integrate_subdivide", integrate_subdivide)
        count_threshold = config.get("count_threshold", count_threshold)
        sigma_cutoff = config.get("sigma_cutoff", sigma_cutoff)
        nproc = config.get("nproc", nproc)
        datastore = config.get("datastore", datastore)
        datastore_bg = config.get("datastore_bg", datastore_bg)
        mca2020 = config.get("mca2020", mca2020)
        if isinstance(integrate_subdivide, list):
            integrate_subdivide = " ".join(str(x) for x in integrate_subdivide)
        if working_dir and not Path(working_dir).is_absolute():
            working_dir = str((search_dir / working_dir).resolve())
        if raw_data_dir and not Path(raw_data_dir).is_absolute():
            raw_data_dir = str((search_dir / raw_data_dir).resolve())

    if raw_data_dir:
        task_wait_for_data_ready(raw_data_dir)

    # Populate crystal_files/background_files from discovered HDF5 masters if empty
    if not skip_setup and run_dials and raw_data_dir and config_path and (not crystal_files or not background_files):
        try:
            populated = populate_deployment_images(str(config_path))
            crystal_files = populated.get("crystal_files") or crystal_files
            background_files = populated.get("background_files") or background_files
        except Exception as e:
            logger.warning("populate_deployment_images failed: %s", e)

    crystal_by_dataset = _normalize_dataset_files(crystal_files, "crystal_files")
    background_by_dataset = _normalize_dataset_files(background_files, "background_files")

    if run_dials:
        if not crystal_by_dataset:
            raise ValueError(
                "crystal_files must be set when run_dials is true. "
                "Either add crystal_files to deployment.json (e.g. [\"images/insulin_2_1\"]), "
                "or ensure raw_data_dir exists and contains *_master.h5 for populate_deployment_images to discover."
            )
        if not background_by_dataset:
            raise ValueError(
                "background_files must be set when run_dials is true. "
                "Either add background_files to deployment.json (e.g. [\"images/insulin_2_bkg\"]), "
                "or ensure raw_data_dir exists and contains *_bg_*master.h5 for populate_deployment_images to discover."
            )
        # Multiple datasets → run each in its own subdir
        if not batch_child and len(crystal_by_dataset) > 1:
            base_wd = (Path(working_dir) if working_dir else Path.cwd()).resolve()
            batch_results = []
            logger.info(
                "Detected multiple datasets in crystal_files (%s). Running sequentially per dataset.",
                ", ".join(sorted(crystal_by_dataset.keys())),
            )
            for dataset in sorted(crystal_by_dataset.keys()):
                ds_crystals = crystal_by_dataset[dataset]
                ds_backgrounds = background_by_dataset.get(dataset, [])
                if not ds_backgrounds:
                    raise ValueError(
                        f"No background_files found for dataset '{dataset}'. "
                        "Provide matching background files for each dataset."
                    )
                ds_wd = base_wd / dataset
                ds_wd.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Running dataset '%s' in %s using crystal=%s background=%s",
                    dataset, ds_wd, ds_crystals[0], ds_backgrounds[0]
                )
                batch_results.append(
                    single_crystal_workflow(
                        working_dir=str(ds_wd),
                        raw_data_dir=raw_data_dir,
                        config_file=None,
                        raw_dir=raw_dir,
                        processed_dir=processed_dir,
                        run_dials=run_dials,
                        crystal_files={dataset: ds_crystals},
                        background_files={dataset: ds_backgrounds},
                        space_group=space_group,
                        refined_expt=refined_expt,
                        background_expt=background_expt,
                        integrate_subdivide=integrate_subdivide,
                        count_threshold=count_threshold,
                        sigma_cutoff=sigma_cutoff,
                        nproc=nproc,
                        datastore=datastore,
                        datastore_bg=datastore_bg,
                        mca2020=mca2020,
                        batch_child=True,
                        skip_setup=skip_setup,
                    )
                )
            logger.info("Finished batched single-crystal workflow for all datasets.")
            return batch_results

    wd = working_dir
    base = Path(working_dir) if working_dir else Path.cwd()
    raw_base = Path(raw_data_dir).resolve() if raw_data_dir else base

    # Run directory setup only when expected project folders already exist
    if skip_setup:
        logger.info("Skipping directory setup (skip_setup=True)")
    elif raw_data_dir:
        setup_base = search_dir.parent.parent
        raw_path = setup_base / raw_dir
        processed_path = setup_base / processed_dir
        if raw_path.is_dir() and processed_path.is_dir():
            directory_setup(
                raw_dir=raw_dir,
                processed_dir=processed_dir,
                working_dir=str(setup_base),
            )
        else:
            logger.info(
                "Skipping directory setup because required folders are missing: raw=%s exists=%s, processed=%s exists=%s",
                raw_path, raw_path.is_dir(), processed_path, processed_path.is_dir(),
            )

    if not run_dials:
        if not (raw_base / refined_expt).exists():
            raise FileNotFoundError(
                f"Expected {refined_expt} in {raw_base}. "
                "Set raw_data_dir, set run_dials=true to run DIALS, or place files in working_dir."
            )
        if not (raw_base / background_expt).exists():
            raise FileNotFoundError(
                f"Expected {background_expt} in {raw_base}. "
                "Set run_dials=true to run DIALS or place files (see examples/insulin-tutorial/README.md)."
            )

    wd_path = Path(wd).resolve()
    if run_dials:
        refined_path = str(wd_path / refined_expt)
        background_path = str(wd_path / background_expt)
    else:
        refined_path = str((raw_base / refined_expt).resolve())
        background_path = str((raw_base / background_expt).resolve())

    results = []
    dials_wd = str(wd_path)

    if run_dials:
        logger.info("Running DIALS: import → find_spots → index → refine, then background import")
        dataset = sorted(crystal_by_dataset.keys())[0]
        crystal_path = str((raw_base / crystal_by_dataset[dataset][0]).resolve())
        background_path_in = str((raw_base / background_by_dataset[dataset][0]).resolve())
        results.append(task_dials_import(
            ["dials.import", crystal_path], dials_wd,
            log_file=str(wd_path / "01_dials_import.log"),
        ))
        results.append(task_dials_find_spots(
            ["dials.find_spots", "imported.expt"], dials_wd,
            log_file=str(wd_path / "02_dials_find_spots.log"),
        ))
        results.append(task_dials_index(
            ["dials.index", "imported.expt", "strong.refl", f"space_group={space_group}"],
            dials_wd,
            log_file=str(wd_path / "03_dials_index.log"),
        ))
        results.append(task_dials_refine(
            ["dials.refine", "indexed.expt", "indexed.refl"], dials_wd,
            log_file=str(wd_path / "04_dials_refine.log"),
        ))
        results.append(task_dials_import_background(
            ["dials.import", background_path_in, "output.experiments=background.expt"],
            dials_wd,
            log_file=str(wd_path / "05_dials_import_background.log"),
        ))

    # mdx2 steps
    wd_str = str(wd_path)
    geom_abs = str(wd_path / "geometry.nxs")
    data_abs = str(wd_path / "data.nxs")
    peaks_abs = str(wd_path / "peaks.nxs")
    mask_abs = str(wd_path / "mask.nxs")
    bkg_data_abs = str(wd_path / "bkg_data.nxs")
    bkg_binned_abs = str(wd_path / "bkg_data_binned.nxs")
    integrated_abs = str(wd_path / "integrated.nxs")
    corrected_abs = str(wd_path / "corrected.nxs")
    scales_abs = str(wd_path / "scales.nxs")
    merged_abs = str(wd_path / "merged.nxs")

    results.append(task_mdx2_import_data(
        "import_data",
        [refined_path, "--nproc", str(nproc), "--datastore", datastore],
        wd_str, log_file=str(wd_path / "06_mdx2_import_data.log"),
    ))
    results.append(task_mdx2_import_geometry(
        "import_geometry", [refined_path], wd_str,
        log_file=str(wd_path / "07_mdx2_import_geometry.log"),
    ))
    results.append(task_mdx2_find_peaks(
        "find_peaks",
        [geom_abs, data_abs, "--nproc", str(nproc), "--count_threshold", str(count_threshold)],
        wd_str, log_file=str(wd_path / "08_mdx2_find_peaks.log"),
    ))
    results.append(task_mdx2_mask_peaks(
        "mask_peaks",
        [geom_abs, data_abs, peaks_abs, "--nproc", str(nproc), "--sigma_cutoff", str(sigma_cutoff)],
        wd_str, log_file=str(wd_path / "09_mdx2_mask_peaks.log"),
    ))
    results.append(task_mdx2_import_data(
        "import_data",
        [background_path, "--outfile", "bkg_data.nxs", "--nproc", str(nproc), "--datastore", datastore_bg],
        wd_str, log_file=str(wd_path / "10_mdx2_import_data_bg.log"),
    ))
    results.append(task_mdx2_bin_image_series(
        "bin_image_series",
        [bkg_data_abs, "10", "20", "20", "--valid_range", "0", "200", "--outfile", "bkg_data_binned.nxs", "--nproc", str(nproc)],
        wd_str, log_file=str(wd_path / "11_mdx2_bin_image_series.log"),
    ))
    results.append(task_mdx2_integrate(
        "integrate",
        [geom_abs, data_abs, "--mask", mask_abs, "--subdivide", *integrate_subdivide.split(), "--nproc", str(nproc)],
        wd_str, log_file=str(wd_path / "12_mdx2_integrate.log"),
    ))
    results.append(task_mdx2_correct(
        "correct",
        [geom_abs, integrated_abs, "--background", bkg_binned_abs],
        wd_str, log_file=str(wd_path / "13_mdx2_correct.log"),
    ))
    scale_args = [corrected_abs]
    if mca2020:
        scale_args.append("--mca2020")
    results.append(task_mdx2_scale(
        "scale", scale_args, wd_str,
        log_file=str(wd_path / "14_mdx2_scale.log"),
    ))
    results.append(task_mdx2_merge(
        "merge", [corrected_abs, "--scale", scales_abs], wd_str,
        log_file=str(wd_path / "15_mdx2_merge.log"),
    ))
    results.append(task_mdx2_map(
        "map",
        [geom_abs, merged_abs, "--limits", "-50", "50", "-50", "50", "0", "0", "--outfile", "slice.nxs"],
        wd_str, log_file=str(wd_path / "16_mdx2_map_slice.log"),
    ))
    results.append(task_mdx2_map(
        "map",
        [geom_abs, merged_abs, "--limits", "-50", "50", "-50", "50", "0.5", "0.5", "--outfile", "offslice.nxs"],
        wd_str, log_file=str(wd_path / "17_mdx2_map_offslice.log"),
    ))

    logger.info("Single-crystal workflow finished.")
    return results


def main() -> None:
    """CLI entry point for mdx2_workflows.pipeline."""
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="Run the single-crystal Prefect workflow.",
    )
    parser.add_argument(
        "--file", "-f", metavar="JSON", default=None,
        help="Config file path. If not set, looks for deployment.json in working dir.",
    )
    parser.add_argument(
        "--working_dir", "-w", metavar="DIR", default=None,
        help="Working directory for outputs. Defaults to cwd.",
    )
    parser.add_argument(
        "--skip_setup", action="store_true", default=False,
        help="Skip directory setup and image discovery. Use when re-running with a manually edited deployment.json.",
    )
    args = parser.parse_args()

    working_dir = args.working_dir or str(Path.cwd())

    config_file = args.file
    if config_file and not Path(config_file).is_absolute():
        config_file = str((Path(working_dir).resolve() / config_file).resolve())

    api_url = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
    os.environ["PREFECT_API_URL"] = api_url
    print(f"Connecting to Prefect API at: {api_url}")
    print(f"Running single-crystal workflow (working_dir={working_dir})")
    if config_file:
        print(f"Config file: {config_file}")

    single_crystal_workflow(working_dir=working_dir, config_file=config_file, skip_setup=args.skip_setup)


if __name__ == "__main__":
    main()
