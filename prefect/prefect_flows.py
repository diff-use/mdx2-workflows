"""
Prefect flows for running mdx2 CLI commands as orchestrated tasks.

This module provides Prefect flows and tasks that wrap mdx2 command-line tools,
making it easy to run pipeline commands with workflow orchestration.
"""

import json
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

from prefect import flow, task
from prefect.logging import get_run_logger


def _copy_dir_structure(src: Path, dst: Path) -> None:
    """Create subdirectories in dst mirroring the directory structure of src. No files are copied."""
    for item in src.rglob("*"):
        if item.is_dir() and not item.name.startswith("."):
            try:
                rel = item.relative_to(src)
                (dst / rel).mkdir(parents=True, exist_ok=True)
            except ValueError:
                pass  # item not under src (should not happen with rglob)


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


def _resolve_raw_entries(raw_dir: Path) -> List[Tuple[str, Path]]:
    """Return list of (name, resolved_path) for each symlink or subdir in raw_dir."""
    entries: List[Tuple[str, Path]] = []
    if not raw_dir.is_dir():
        return entries
    for p in sorted(raw_dir.iterdir()):
        if p.name.startswith("."):
            continue
        if p.is_symlink():
            target = p.resolve()
            if target.is_dir():
                entries.append((p.name, target))
        elif p.is_dir():
            entries.append((p.name, p.resolve()))
    return entries


@task(name="populate-deployment-images", log_prints=True)
def populate_deployment_images(deployment_file: str) -> dict:
    """
    Populate deployment.json with paths discovered from raw HDF5 masters:
    - background_files: list of *_bg_*master.h5 relative to raw_data_dir
    - crystal_files: list of non-bg *_master.h5 relative to raw_data_dir
    """
    from mdx2.command_line.pipeline import _populate_deployment_images
    return _populate_deployment_images(deployment_file)


def _tee_output_to_file(result: subprocess.CompletedProcess, log_file: str) -> None:
    """Write stdout and stderr to log_file (tee-like behavior)."""
    with open(log_file, "w") as f:
        if result.stdout:
            f.write(result.stdout)
        if result.stderr:
            if result.stdout and not result.stdout.endswith("\n"):
                f.write("\n")
            f.write(result.stderr)


@task(name="run-mdx2-command", log_prints=True)
def run_mdx2_cli_command(
    command: str,
    args: List[str],
    working_dir: Optional[str] = None,
    conda_env: str = "mdx2-dev",
    log_file: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """
    Run an mdx2 CLI command as a Prefect task.
    
    Args:
        command: The mdx2 command to run (e.g., 'map', 'integrate', 'scale')
        args: List of command-line arguments to pass to the command
        working_dir: Working directory for the command (default: current directory)
        conda_env: Conda environment name to activate (default: 'mdx2-dev')
    
    Returns:
        CompletedProcess object with returncode, stdout, stderr
    
    Example:
        >>> result = run_mdx2_cli_command(
        ...     command="map",
        ...     args=["geom.nxs", "hkl.nxs", "--outfile", "map.nxs"]
        ... )
    """
    logger = get_run_logger()
    
    # Build the command: activate conda env and run the module
    cmd = [
        "micromamba", "run", "-n", conda_env,
        "python", "-m", f"mdx2.command_line.{command}"
    ] + args
    
    logger.info(f"Running command: {' '.join(cmd)}")
    if working_dir:
        logger.info(f"Working directory: {working_dir}")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=working_dir,
            capture_output=True,
            text=True,
            check=False,  # Don't raise on non-zero exit, we'll handle it
        )
        
        if result.stdout:
            logger.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR:\n{result.stderr}")
        if log_file:
            _tee_output_to_file(result, log_file)
        
        if result.returncode != 0:
            logger.error(f"Command failed with return code {result.returncode}")
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )
        
        logger.info(f"Command '{command}' completed successfully")
        return result
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Command execution failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error running command: {e}")
        raise


@task(name="run-conda-command", log_prints=True)
def run_conda_command(
    argv: List[str],
    working_dir: Optional[str] = None,
    conda_env: str = "mdx2-dev",
    log_file: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """
    Run an arbitrary command in the conda environment (e.g. DIALS commands).
    argv: e.g. ["dials.import", "images/insulin_2_1"]
    """
    logger = get_run_logger()
    cmd = ["micromamba", "run", "-n", conda_env] + argv
    logger.info("Running: %s (cwd=%s)", " ".join(cmd), working_dir)
    try:
        result = subprocess.run(
            cmd,
            cwd=working_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("STDOUT:\n%s", result.stdout)
        if result.stderr:
            logger.warning("STDERR:\n%s", result.stderr)
        if log_file:
            _tee_output_to_file(result, log_file)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )
        return result
    except subprocess.CalledProcessError as e:
        logger.error("Command failed: %s", e)
        raise


@flow(name="mdx2-pipeline", log_prints=True)
def custom_workflow(
    commands: List[dict],
    working_dir: Optional[str] = None,
    conda_env: str = "mdx2-dev",
) -> List[subprocess.CompletedProcess]:
    """
    Run a sequence of mdx2 CLI commands as a Prefect flow.
    
    Args:
        commands: List of command dictionaries, each with:
            - 'command': CLI command name (e.g., 'integrate', 'scale', 'map')
            - 'args': List of arguments for the command
        working_dir: Working directory for all commands
        conda_env: Conda environment name
    
    Returns:
        List of CompletedProcess objects, one per command
    
    Example:
        >>> results = mdx2_pipeline_flow([
        ...     {
        ...         "command": "integrate",
        ...         "args": ["geom.nxs", "data.nxs", "--outfile", "integrated.nxs"]
        ...     },
        ...     {
        ...         "command": "scale",
        ...         "args": ["integrated.nxs", "--outfile", "scaled.nxs"]
        ...     },
        ...     {
        ...         "command": "map",
        ...         "args": ["geom.nxs", "scaled.nxs", "--outfile", "map.nxs"]
        ...     }
        ... ])
    """
    logger = get_run_logger()
    logger.info(f"Starting mdx2 pipeline with {len(commands)} commands")
    
    results = []
    for i, cmd_config in enumerate(commands, 1):
        command = cmd_config["command"]
        args = cmd_config.get("args", [])
        
        logger.info(f"Step {i}/{len(commands)}: Running '{command}'")
        
        result = run_mdx2_cli_command(
            command=command,
            args=args,
            working_dir=working_dir,
            conda_env=conda_env,
        )
        results.append(result)
    
    logger.info(f"Pipeline completed successfully with {len(results)} commands")
    return results


# Public alias for the pipeline flow
mdx2_pipeline_flow = custom_workflow


# Convenience flows for common single-command operations
@flow(name="mdx2-map", log_prints=True)
def map_flow(
    geom: str,
    hkl: str,
    outfile: str = "map.nxs",
    symmetry: bool = True,
    limits: tuple = (0, 10, 0, 10, 0, 10),
    signal: str = "intensity",
    working_dir: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """Prefect flow for running mdx2 map command."""
    args = [geom, hkl, "--outfile", outfile]
    if not symmetry:
        args.append("--no-symmetry")
    if limits != (0, 10, 0, 10, 0, 10):
        args.extend(["--limits"] + [str(x) for x in limits])
    if signal != "intensity":
        args.extend(["--signal", signal])
    
    return run_mdx2_cli_command("map", args, working_dir=working_dir)


@flow(name="mdx2-integrate", log_prints=True)
def integrate_flow(
    geom: str,
    data: str,
    outfile: str = "integrated.nxs",
    mask: Optional[str] = None,
    subdivide: tuple = (1, 1, 1),
    max_spread: float = 1.0,
    nproc: int = 1,
    working_dir: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """Prefect flow for running mdx2 integrate command."""
    args = [geom, data, "--outfile", outfile]
    if mask:
        args.extend(["--mask", mask])
    if subdivide != (1, 1, 1):
        args.extend(["--subdivide"] + [str(x) for x in subdivide])
    if max_spread != 1.0:
        args.extend(["--max-spread", str(max_spread)])
    if nproc != 1:
        args.extend(["--nproc", str(nproc)])
    
    return run_mdx2_cli_command("integrate", args, working_dir=working_dir)


@flow(name="mdx2-scale", log_prints=True)
def scale_flow(
    hkl: str,
    outfile: str = "scaled.nxs",
    working_dir: Optional[str] = None,
    **kwargs,
) -> subprocess.CompletedProcess:
    """Prefect flow for running mdx2 scale command."""
    args = [hkl, "--outfile", outfile]
    # Add any additional kwargs as command-line arguments
    for key, value in kwargs.items():
        if value is not None:
            args.extend([f"--{key.replace('_', '-')}", str(value)])
    
    return run_mdx2_cli_command("scale", args, working_dir=working_dir)


@flow(name="directory-setup", log_prints=True)
def directory_setup(
    raw_dir: str = "raw_data",
    processed_dir: str = "processed_data",
    working_dir: Optional[str] = None,
) -> List[Path]:
    """
    Ensure raw_data and processed_data exist and that each crystal directory from
    raw_data has a corresponding directory in processed_data. Creates any missing
    directories. No DIALS or mdx2 commands are run.
    """
    logger = get_run_logger()
    base = Path(working_dir) if working_dir else Path.cwd()
    raw_path = (base / raw_dir).resolve()
    processed_path = (base / processed_dir).resolve()

    # Ensure raw_data and processed_data exist
    raw_path.mkdir(parents=True, exist_ok=True)
    processed_path.mkdir(parents=True, exist_ok=True)

    entries = _resolve_raw_entries(raw_path)
    if not entries:
        logger.warning("No symlinks or subdirectories found in %s", raw_path)
        return []

    logger.info("Found %s entries in raw_data: %s", len(entries), [e[0] for e in entries])
    created: List[Path] = []

    for name, resolved_path in entries:
        out_path = processed_path / name
        out_path.mkdir(parents=True, exist_ok=True)
        _copy_dir_structure(resolved_path, out_path)
        deployment_file = out_path / "deployment.json"
        if not deployment_file.exists():
            config = _default_deployment_json(name, raw_dir, processed_dir)
            with open(deployment_file, "w") as f:
                json.dump(config, f, indent=2)
                f.write("\n")
            logger.info("Created %s", deployment_file)
        # Fill crystal_files/background_files from discovered HDF5 masters (if present)
        populate_deployment_images(str(deployment_file))
        created.append(out_path)
        logger.info("Created directory: %s (with subdirectory structure)", out_path)

    return created


if __name__ == "__main__":
    import os
    from prefect import serve

    from mdx2.command_line.pipeline import single_crystal_workflow

    api_url = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
    os.environ["PREFECT_API_URL"] = api_url
    print(f"Connecting to Prefect API at: {api_url}")

    serve(
        custom_workflow.to_deployment(name="custom-workflow"),
        directory_setup.to_deployment(name="directory-setup"),
        single_crystal_workflow.to_deployment(name="single-crystal-workflow"),
    )
