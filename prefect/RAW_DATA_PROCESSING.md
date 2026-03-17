# Processing raw_data into processed_data

Run the **Prefect flow** so that data from **symlinks in `raw_data/`** is processed and results are written to **`processed_data/<name>/`**.

## Directory layout

- **`raw_data/`** – One symlink (or directory) per dataset, e.g.:
  - `insulin` → `/mnt/chess/20260218/insulin`
  - `mac1_c2` → `/mnt/chess/20260218/mac1_c2`
- **`processed_data/`** – One folder per dataset, e.g.:
  - `processed_data/insulin/` – `integrated.nxs`, `scaled.nxs`, `map.nxs`
  - `processed_data/mac1_c2/` – same

The pipeline per dataset is: **integrate** → **scale** → **map**.

## Docker: symlinks must resolve

Symlinks point to absolute paths (e.g. `/mnt/chess/20260218/insulin`). In **`docker-compose.yml`** the raw data root is mounted so they resolve in the container:

```yaml
volumes:
  - /mnt/chess:/mnt/chess
```

If your symlinks point elsewhere, change the host path to match.

## Running the pipeline (Prefect only)

### 1. From the Prefect UI

1. Start the stack: `docker compose up` (from the `mdx2/` directory).
2. Open `http://localhost:4200`.
3. Go to **Deployments** → **process-raw-data-deployment** → **Run**.
4. Set parameters if needed, e.g.:
   - `working_dir`: `"/home/workspace"` (so `raw_data` and `processed_data` are under the workspace mount)
   - `raw_dir`: `"raw_data"`
   - `processed_dir`: `"processed_data"`
   - `geom_file`: `"geometry.nxs"`
   - `data_file`: `"data.nxs"`
   - `nproc`: `4`

### 2. From Python

Run from mdx2 repo root or add `prefect/` to PYTHONPATH:

```python
from prefect_flows import process_raw_data_flow

process_raw_data_flow(
    raw_dir="raw_data",
    processed_dir="processed_data",
    geom_file="geometry.nxs",
    data_file="data.nxs",
    working_dir="/home/workspace",  # when running in Docker
    nproc=4,
)
```

### 3. Via Prefect CLI

```bash
export PREFECT_API_URL=http://localhost:4200/api
prefect deployment run 'process-raw-data/process-raw-data-deployment' \
  --param working_dir=/home/workspace
```

## Flow parameters

| Parameter       | Default           | Description |
|----------------|-------------------|-------------|
| `raw_dir`      | `raw_data`        | Directory containing symlinks to raw datasets. |
| `processed_dir`| `processed_data`  | Directory where `/<name>/` output folders are created. |
| `geom_file`    | `geometry.nxs`    | Geometry NeXus file name inside each raw dataset. |
| `data_file`    | `data.nxs`        | Image series NeXus file name inside each raw dataset. |
| `mask_file`    | `None`            | Optional mask file name inside each raw dataset. |
| `working_dir`  | `None` (cwd)      | Base path for `raw_dir` and `processed_dir`. Use `/home/workspace` in Docker. |
| `nproc`       | 1                 | Number of processes for integration. |
| `fail_fast`    | False             | Stop on first dataset failure instead of continuing. |

## Outputs per dataset

For each entry `raw_data/<name>` → `processed_data/<name>/`:

- `integrated.nxs` – from **integrate** (geom + data → hkl_table)
- `scaled.nxs` – from **scale** (integrated → scaled)
- `map.nxs` – from **map** (geom + scaled → map)
