# Using Prefect with mdx2 CLI Commands

This guide explains how to use Prefect workflow orchestration with mdx2 CLI commands.

## Processing raw_data → processed_data

To process all symlinks in `raw_data/` and write results to `processed_data/<name>/`, use the **`process-raw-data`** Prefect flow. See **[RAW_DATA_PROCESSING.md](RAW_DATA_PROCESSING.md)** for:

- Mounting the raw data root in Docker so symlinks resolve (e.g. `/mnt/chess`)
- Triggering the flow from the UI, Python, or Prefect CLI
- Flow parameters (geom_file, data_file, working_dir, nproc, etc.)

## Single-crystal example pipeline

To run all steps from the **insulin-tutorial** (single-crystal) example in one go:

```bash
# With a config file (e.g. in mdx2-dev env or Docker)
mdx2.pipeline --file deployment.json --working_dir /path/to/processed_data

# Or as a module (--working_dir defaults to examples/insulin-tutorial if in repo, else cwd)
python -m mdx2.command_line.pipeline --file deployment.json --working_dir /path/to/processed_data
```

**Run in background with screen:** `screen -S mdx2-pipeline`, then run one of the commands above (e.g. `mdx2.pipeline --file deployment.json`). Detach with **Ctrl+A**, **D**. Reattach with `screen -r mdx2-pipeline` to see logs or when it finishes.

Or trigger the **single-crystal-example** deployment from the Prefect UI. The flow runs **DIALS** first (when `run_dials` is true): dials.import → find_spots → index → refine, then background import; then **mdx2**: import_geometry → import_data → find_peaks → mask_peaks → background bin → integrate → correct → scale → merge → map. Set `run_dials: false` in your config if refined.expt and background.expt already exist. Config can set `crystal_files`, `background_files`, `space_group` (e.g. 199 for insulin). See `examples/insulin-tutorial/README.md` for the dataset.

**Config file:** Use **`--file`** to pass any JSON config path (e.g. `deployment.json`). If you omit `--file`, the flow looks for **`deployment.json`** in the working directory. Relative paths in the config (e.g. `working_dir`, `raw_data_dir`) are resolved from the config file's directory. Supported keys: `working_dir`, `raw_data_dir`, `run_dials`, `crystal_files`, `background_files`, `space_group`, `refined_expt`, `background_expt`, `integrate_subdivide`, `count_threshold`, `sigma_cutoff`, `nproc`, `datastore`, `datastore_bg`, `mca2020`.

**Example config (save as `deployment.json`):**
```json
{
  "working_dir": ".",
  "raw_data_dir": "../raw_data",
  "refined_expt": "refined.expt",
  "background_expt": "background.expt",
  "integrate_subdivide": "4 4 4",
  "count_threshold": 20,
  "sigma_cutoff": 3
}
```


## Running in the background with screen

Use **screen** so the process keeps running after you close the terminal (until you shut down the machine or kill the session).

- **Start a named session:** `screen -S mdx2`
- **Run your command** in that terminal (e.g. `docker compose up` or `mdx2.pipeline --file ...`).
- **Detach** (leave it running): press **Ctrl+A**, then **D**.
- **Reattach later:** `screen -r mdx2` (or `screen -r` if only one session).
- **List sessions:** `screen -ls`
- **Kill a session:** from inside the session, type `exit`, or run `screen -S mdx2 -X quit`.

If `screen` is not installed: `sudo apt install screen` (Debian/Ubuntu) or equivalent.

---

## Quick Start

1. **Start the Prefect server and flow runner:**
   ```bash
   docker compose up
   ```
   **Run in background with screen:** Start a session (`screen -S mdx2`), run `docker compose up`, then detach with **Ctrl+A**, **D**. Reattach anytime with `screen -r mdx2`.

   This starts:
   - Prefect server UI at `http://localhost:4200`
   - Flow runner that serves mdx2 Prefect flows
   - mdx2 container for manual CLI access

2. **Access the Prefect UI:**
   - Open `http://localhost:4200` in your browser
   - You'll see the `mdx2-pipeline-deployment` available

## Running Commands

### Option 1: Via Prefect UI

**Run in background with screen:** Start the stack in a screen session first (see Quick Start). Detach with **Ctrl+A**, **D**. Open the UI in your browser from any machine; flow runs execute in the background in the flow-runner container. Reattach to the screen session with `screen -r mdx2` to see server/runner logs.

1. Go to `http://localhost:4200`
2. Navigate to "Deployments"
3. Find `mdx2-pipeline-deployment`
4. Click "Run" to trigger a flow run
5. Provide parameters as JSON:
   ```json
   {
     "commands": [
       {
         "command": "integrate",
         "args": ["geom.nxs", "data.nxs", "--outfile", "integrated.nxs"]
       },
       {
         "command": "scale",
         "args": ["integrated.nxs", "--outfile", "scaled.nxs"]
       }
     ]
   }
   ```

### Option 2: Via Prefect CLI (from host machine)

**Run in background with screen:** Run the stack in screen (Quick Start) so the server and flow runner keep running. In the same or another terminal, run the `prefect deployment run` command below; the triggered run executes in the flow-runner container. To keep the trigger step itself in screen (e.g. to capture output), use `screen -S prefect-trigger`, run the commands, then detach with **Ctrl+A**, **D**.

First, configure Prefect to connect to the server:
```bash
export PREFECT_API_URL=http://localhost:4200/api
```

Then run a deployment:
```bash
prefect deployment run mdx2-pipeline/mdx2-pipeline-deployment
```

### Option 3: Via Python Script

**Run in background with screen:** `screen -S mdx2-python`, run your script (e.g. `python my_pipeline.py`), then detach with **Ctrl+A**, **D**. Reattach with `screen -r mdx2-python` to see output or when it finishes.

Create a Python script that uses the flows. Run from mdx2 repo root, or add the `prefect/` directory to `PYTHONPATH`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / "prefect"))  # if needed
from prefect_flows import mdx2_pipeline_flow

# Run a pipeline
results = mdx2_pipeline_flow([
    {
        "command": "integrate",
        "args": ["geom.nxs", "data.nxs", "--outfile", "integrated.nxs"]
    },
    {
        "command": "scale",
        "args": ["integrated.nxs", "--outfile", "scaled.nxs"]
    }
])
```

### Option 4: Direct CLI Execution (inside container)

**Run in background with screen:** `screen -S mdx2-cli`, run the `docker compose exec ...` command below, then detach with **Ctrl+A**, **D**. Reattach with `screen -r mdx2-cli` to see output or when it finishes.

Execute commands directly in the mdx2 container:
```bash
docker compose exec container micromamba run -n mdx2-dev python -m mdx2.command_line.map geom.nxs hkl.nxs --outfile map.nxs
```

## Available Prefect Flows

### `mdx2_pipeline_flow`
Run a sequence of mdx2 CLI commands:
```python
from prefect_flows import mdx2_pipeline_flow

results = mdx2_pipeline_flow([
    {"command": "integrate", "args": ["geom.nxs", "data.nxs"]},
    {"command": "scale", "args": ["integrated.nxs"]},
])
```

### `map_flow`
Convenience flow for map command:
```python
from prefect_flows import map_flow

result = map_flow(
    geom="geom.nxs",
    hkl="hkl.nxs",
    outfile="map.nxs",
    symmetry=True,
    signal="intensity"
)
```

### `integrate_flow`
Convenience flow for integrate command:
```python
from prefect_flows import integrate_flow

result = integrate_flow(
    geom="geom.nxs",
    data="data.nxs",
    outfile="integrated.nxs",
    nproc=4
)
```

### `scale_flow`
Convenience flow for scale command:
```python
from prefect_flows import scale_flow

result = scale_flow(
    hkl="integrated.nxs",
    outfile="scaled.nxs"
)
```

### `run_mdx2_cli_command`
Generic task to run any mdx2 CLI command:
```python
from prefect_flows import run_mdx2_cli_command

result = run_mdx2_cli_command(
    command="map",
    args=["geom.nxs", "hkl.nxs", "--outfile", "map.nxs"]
)
```

## Available CLI Commands

All mdx2 CLI commands can be run via Prefect:
- `integrate` - Integrate counts on a Miller index grid
- `scale` - Scale integrated data
- `map` - Create a map from hkl table
- `correct` - Apply corrections
- `merge` - Merge multiple datasets
- `find_peaks` - Find peaks in data
- `mask_peaks` - Mask peaks
- `import_data` - Import data
- `import_geometry` - Import geometry
- `bin_image_series` - Bin image series
- `reintegrate` - Reintegrate data
- `tree` - Show data tree structure

## Working Directory

By default, commands run in the current working directory. You can specify a different directory:

```python
result = run_mdx2_cli_command(
    command="map",
    args=["geom.nxs", "hkl.nxs"],
    working_dir="/path/to/data"
)
```

## Monitoring and Logs

- **Prefect UI**: View flow runs, logs, and status at `http://localhost:4200`
- **Container logs**: `docker compose logs flow-runner`
- **Command logs**: Each command creates a log file like `mdx2.map.log`

## Running from Sibling Directories

If you have a directory at the same level as `mdx2/` (e.g., `/data/CHESS_20260218/insulin/`), you can run mdx2 commands from that directory. To run any of the commands below in the background, use **screen** as in the options above (e.g. `screen -S mdx2`, run the command, then **Ctrl+A**, **D**).

### Option 1: Using Prefect Flows with working_dir

When calling Prefect flows, specify the `working_dir` parameter:

```python
from prefect_flows import run_mdx2_cli_command

# Run from a sibling directory
result = run_mdx2_cli_command(
    command="map",
    args=["geom.nxs", "hkl.nxs", "--outfile", "map.nxs"],
    working_dir="/home/workspace/insulin"  # Path inside container
)
```

The parent directory is mounted at `/home/workspace` in the container, so:
- Host path: `/data/CHESS_20260218/insulin/`
- Container path: `/home/workspace/insulin/`

### Option 2: Direct Container Access

Execute commands directly in the container from your sibling directory:

```bash
# From your sibling directory (e.g., /data/CHESS_20260218/insulin/)
docker compose -f ../mdx2/docker-compose.yml exec container \
  micromamba run -n mdx2-dev python -m mdx2.command_line.map \
  geom.nxs hkl.nxs --outfile map.nxs
```

Or change into the directory first:

```bash
docker compose -f ../mdx2/docker-compose.yml exec -w /home/workspace/insulin container \
  micromamba run -n mdx2-dev python -m mdx2.command_line.map \
  geom.nxs hkl.nxs --outfile map.nxs
```

### Option 3: Using Relative Paths in Prefect

When using Prefect flows, you can use relative paths from the mounted workspace:

```python
from prefect_flows import mdx2_pipeline_flow

# Use relative paths - they'll be resolved from the working_dir
results = mdx2_pipeline_flow(
    commands=[
        {
            "command": "integrate",
            "args": ["geom.nxs", "data.nxs", "--outfile", "integrated.nxs"]
        }
    ],
    working_dir="/home/workspace/insulin"
)
```
