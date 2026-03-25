# mdx2-workflows

The repository contains the infrastructure and data processing workflows for [mdx2](https://github.com/diff-use/mdx2) to process diffuse x-ray crystallography data in an automated fashion. Though not necessary for running mdx2, users will benefit from the various data pipelines and custom pipeline feature in [Prefect](https://www.prefect.io/). Additionally, `.github` primarily houses GitHub actions to ensure changes won't disrupt the current version of mdx2-workflows, and secondarily provides transparency on the latest Dockerfile version published on [Docker Hub](https://hub.docker.com/repository/docker/diffuseproject/mdx2/general).

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)

## Quick Start

```bash
git clone https://github.com/diff-use/mdx2-workflows.git
cd mdx2-workflows
docker compose up --build
```

This starts:
- **Prefect server** — UI and API at `http://localhost:4200`
- **Flow runner** — serves mdx2 Prefect flows as deployments
- **Container** — idle mdx2 container for manual CLI access via `docker compose exec`

## Choosing an mdx2 Version

There are two modes for getting mdx2 into the containers, controlled by the `MDX2_DOCKERFILE` variable:

### Docker Hub (default) — fast

Pulls a pre-built image from Docker Hub and adds Prefect on top. This is the default; no extra configuration needed.

```bash
# Use a specific Docker Hub tag
MDX2_IMAGE_TAG=1.0.0 docker compose up --build

# Or set it persistently
echo "MDX2_IMAGE_TAG=1.0.0" >> .env
docker compose up --build
```

<!-- TODO(mdx2-versioning): Once the mdx2 GHA docker.yml tags images on release,
document available tags here. -->
<!-- TODO(mdx2-releases): Once GitHub releases exist, link to
https://github.com/diff-use/mdx2/releases for changelogs. -->

### Build from source — for development

Clones the mdx2 repo at a specific commit and builds the entire environment from scratch (conda env, dependencies, mdx2). Slower, but guarantees the environment matches the code.

```bash
# Build from a specific commit
MDX2_DOCKERFILE=Dockerfile.source MDX2_COMMIT=542f0f19 docker compose up --build

# Or from a branch
MDX2_DOCKERFILE=Dockerfile.source MDX2_COMMIT=main docker compose up --build

# Or set persistently in .env
cat >> .env <<EOF
MDX2_DOCKERFILE=Dockerfile.source
MDX2_COMMIT=542f0f195e6de55ff734913c5bbd4cdb30beb2e6
EOF
docker compose up --build
```

### Configuration summary

| Variable | Used by | Purpose | Default |
|----------|---------|---------|---------|
| `MDX2_DOCKERFILE` | both | Selects the Dockerfile | `Dockerfile` |
| `MDX2_IMAGE_TAG` | `Dockerfile` | Docker Hub tag for `diffuseproject/mdx2` | `1.0.0` |
| `MDX2_COMMIT` | `Dockerfile.source` | Git commit, branch, or tag to build from | `main` |

## Services

| Service | Purpose | Access |
|---------|---------|--------|
| `prefect-server` | Prefect UI, API, and orchestration backed by PostgreSQL | `http://localhost:4200` |
| `flow-runner` | Runs `prefect_flows.py` to serve mdx2 flows as Prefect deployments | Internal |
| `container` | Idle mdx2 container for ad-hoc CLI commands via `docker compose exec` | Port 8888 (JupyterHub) |

## Running mdx2 Commands

### Via Prefect UI

1. Open `http://localhost:4200`
2. Navigate to **Deployments**
3. Select a deployment (e.g. `custom-workflow`) and click **Run**

### Via `docker compose exec` (ad-hoc CLI)

```bash
docker compose exec container micromamba run -n mdx2-dev \
  python -m mdx2.command_line.map geom.nxs hkl.nxs --outfile map.nxs
```

### Via Python

See [prefect/PREFECT_USAGE.md](prefect/PREFECT_USAGE.md) for detailed usage including pipeline orchestration, single-crystal workflows, and running from sibling directories.

## Data Volumes

The `flow-runner` and `container` services mount host directories for data access. Edit `docker-compose.yml` to match your paths:

```yaml
volumes:
  - ..:/home/workspace          # Parent directory (sibling dirs accessible)
  - /mnt/chess:/mnt/chess       # Raw data root (so symlinks resolve)
  - .:/home/dev                 # This repo (workflow code)
```

If your raw data symlinks point somewhere other than `/mnt/chess`, update the mount accordingly. See [prefect/RAW_DATA_PROCESSING.md](prefect/RAW_DATA_PROCESSING.md) for details.

## Project Structure

```
mdx2-workflows/
├── docker-compose.yml        # Service definitions (Prefect server, flow runner, mdx2 container)
├── Dockerfile                # Docker Hub mode — pulls diffuseproject/mdx2 + adds Prefect
├── Dockerfile.source         # Source mode — clones mdx2 at a commit, builds from scratch
├── .env.example              # Environment variable template
├── env.yaml                  # Conda environment spec (used by Dockerfile.source)
├── README.md
└── prefect/
    ├── prefect_flows.py      # Prefect flows wrapping mdx2 CLI commands
    ├── run_mdx2_prefect.py   # CLI helper for running flows
    ├── PREFECT_USAGE.md      # Detailed Prefect usage guide
    └── RAW_DATA_PROCESSING.md
```
