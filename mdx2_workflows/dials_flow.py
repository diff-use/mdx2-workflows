"""
Prefect flow for the DIALS-only portion of the single-crystal workflow.

Runs: dials.import → find_spots → spot_counts_per_image → index → refine →
integrate → scale → export → merge → import (background).
Produces scaled/merged outputs and background.expt, then stops (no mdx2 steps).

Config is loaded from deployment.json the same way as the full pipeline.
Use --file to specify a config file or --working_dir to set the output
directory.

Run from the mdx2-dev env:
  mdx2_workflows.dials_flow --file deployment.json
  mdx2_workflows.dials_flow --working_dir /path/to/processed_data/insulin
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from prefect import flow
from prefect.logging import get_run_logger

from mdx2_workflows.pipeline import (  # noqa: E402
    _normalize_dataset_files,
    directory_setup,
    load_single_crystal_config,
    load_single_crystal_config_from_file,
    populate_deployment_images,
    task_dials_export,
    task_dials_find_spots,
    task_dials_import,
    task_dials_import_background,
    task_dials_index,
    task_dials_integrate,
    task_dials_merge,
    task_dials_refine,
    task_dials_scale,
    task_dials_spot_counts_per_image,
    task_wait_for_data_ready,
)


@flow(name="dials-workflow", log_prints=True)
def dials_workflow(
    working_dir: Optional[str] = None,
    raw_data_dir: Optional[str] = None,
    config_file: Optional[str] = None,
    raw_dir: str = "raw_data",
    processed_dir: str = "processed_data",
    crystal_files: Optional[Any] = None,
    background_files: Optional[Any] = None,
    space_group: int = 199,
    skip_setup: bool = False,
) -> list:
    """
    Run only the DIALS steps: import → find_spots → spot_counts_per_image →
    index → refine → integrate → scale → export → merge, then import the background
    dataset. Produces scaled/merged outputs and background.expt in
    working_dir.

    Config file (deployment.json) can override all parameters.
    Set skip_setup=true to bypass directory_setup and image discovery,
    useful when re-running with a manually edited deployment.json.
    """
    logger = get_run_logger()

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
        crystal_files = config.get("crystal_files", crystal_files)
        background_files = config.get("background_files", background_files)
        space_group = config.get("space_group", space_group)
        if working_dir and not Path(working_dir).is_absolute():
            working_dir = str((search_dir / working_dir).resolve())
        if raw_data_dir and not Path(raw_data_dir).is_absolute():
            raw_data_dir = str((search_dir / raw_data_dir).resolve())

    if raw_data_dir:
        task_wait_for_data_ready(raw_data_dir)

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
                "Skipping directory setup: raw=%s exists=%s, processed=%s exists=%s",
                raw_path, raw_path.is_dir(), processed_path, processed_path.is_dir(),
            )

    if not skip_setup and raw_data_dir and config_path and (not crystal_files or not background_files):
        try:
            populated = populate_deployment_images(str(config_path))
            crystal_files = populated.get("crystal_files") or crystal_files
            background_files = populated.get("background_files") or background_files
        except Exception as e:
            logger.warning("populate_deployment_images failed: %s", e)

    crystal_by_dataset = _normalize_dataset_files(crystal_files, "crystal_files")
    background_by_dataset = _normalize_dataset_files(background_files, "background_files")

    if not crystal_by_dataset:
        raise ValueError(
            "crystal_files must be set. "
            "Add crystal_files to deployment.json or ensure raw_data_dir contains *_master.h5."
        )
    if not background_by_dataset:
        raise ValueError(
            "background_files must be set. "
            "Add background_files to deployment.json or ensure raw_data_dir contains *_bg_*master.h5."
        )

    wd_path = (Path(working_dir) if working_dir else Path.cwd()).resolve()
    raw_base = Path(raw_data_dir).resolve() if raw_data_dir else wd_path
    dials_wd = str(wd_path)

    all_results: List = []

    for dataset in sorted(crystal_by_dataset.keys()):
        ds_backgrounds = background_by_dataset.get(dataset, [])
        if not ds_backgrounds:
            raise ValueError(
                f"No background_files found for dataset '{dataset}'. "
                "Provide matching background files for each dataset."
            )

        ds_wd_path = wd_path / dataset
        ds_wd_path.mkdir(parents=True, exist_ok=True)
        ds_wd = str(ds_wd_path)

        crystal_path = str((raw_base / crystal_by_dataset[dataset][0]).resolve())
        background_path = str((raw_base / ds_backgrounds[0]).resolve())

        logger.info(
            "Running DIALS for dataset '%s': crystal=%s, background=%s, cwd=%s",
            dataset, crystal_path, background_path, ds_wd,
        )

        results = []
        results.append(task_dials_import(
            ["dials.import", crystal_path], ds_wd,
            log_file=str(ds_wd_path / "01_dials_import.log"),
        ))
        results.append(task_dials_find_spots(
            ["dials.find_spots", "imported.expt"], ds_wd,
            log_file=str(ds_wd_path / "02_dials_find_spots.log"),
        ))
        results.append(task_dials_spot_counts_per_image(
            ["dials.spot_counts_per_image", "imported.expt", "strong.refl",
             "json=spot_counts_per_image.json"],
            ds_wd,
            log_file=str(ds_wd_path / "03_dials_spot_counts_per_image.log"),
        ))
        results.append(task_dials_index(
            ["dials.index", "imported.expt", "strong.refl", f"space_group={space_group}"],
            ds_wd,
            log_file=str(ds_wd_path / "04_dials_index.log"),
        ))
        results.append(task_dials_refine(
            ["dials.refine", "indexed.expt", "indexed.refl"], ds_wd,
            log_file=str(ds_wd_path / "05_dials_refine.log"),
        ))
        results.append(task_dials_integrate(
            ["dials.integrate", "refined.expt", "refined.refl"], ds_wd,
            log_file=str(ds_wd_path / "06_dials_integrate.log"),
        ))
        results.append(task_dials_scale(
            ["dials.scale", "integrated.expt", "integrated.refl", "absorption_level=medium"],
            ds_wd,
            log_file=str(ds_wd_path / "07_dials_scale.log"),
        ))
        results.append(task_dials_export(
            ["dials.export", "scaled.expt", "scaled.refl"], ds_wd,
            log_file=str(ds_wd_path / "08_dials_export.log"),
        ))
        results.append(task_dials_merge(
            ["dials.merge", "scaled.expt", "scaled.refl"], ds_wd,
            log_file=str(ds_wd_path / "09_dials_merge.log"),
        ))
        results.append(task_dials_import_background(
            ["dials.import", background_path, "output.experiments=background.expt"],
            ds_wd,
            log_file=str(ds_wd_path / "10_dials_import_background.log"),
        ))
        all_results.extend(results)

    logger.info("DIALS workflow finished for %d dataset(s).", len(crystal_by_dataset))
    return all_results


def main() -> None:
    """CLI entry point for mdx2_workflows.dials_flow."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the DIALS-only Prefect workflow.",
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
        help="Skip directory setup and image discovery.",
    )
    args = parser.parse_args()

    working_dir = args.working_dir or str(Path.cwd())

    config_file = args.file
    if config_file and not Path(config_file).is_absolute():
        config_file = str((Path(working_dir).resolve() / config_file).resolve())

    api_url = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
    os.environ["PREFECT_API_URL"] = api_url
    print(f"Connecting to Prefect API at: {api_url}")
    print(f"Running DIALS workflow (working_dir={working_dir})")
    if config_file:
        print(f"Config file: {config_file}")

    dials_workflow(working_dir=working_dir, config_file=config_file, skip_setup=args.skip_setup)


if __name__ == "__main__":
    main()
