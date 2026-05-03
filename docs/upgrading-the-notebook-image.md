# Upgrading the notebook image (`diffuseproject/mdx2:<tag>`)

This guide covers how to publish a new version of the JupyterHub singleuser image — the image users get when they spawn a notebook in the Diffuse JupyterHub deployment.

If you just want the runbook, jump to **[Upgrade scenarios](#upgrade-scenarios)** below.

## When to use this guide

Use this guide when you want to:

- Ship a new version of mdx2 (e.g. v1.0.4) to JupyterHub users
- Refresh the conda environment (DIALS bump, security patches, quarterly refresh)
- Add an OS package to the image (apt deps for new tooling)
- Fix a bug in the image's build process

**Do not use this guide** for:

- Pure documentation changes (won't trigger the workflow on `main`; no image rebuild needed)
- Changes to other Dockerfiles in the repo (`Dockerfile`, `Dockerfile.source`, `.github/Dockerfile`) — those serve different purposes and have their own (or no) build pipelines

## How the pipeline is wired

```
.github/workflows/docker.yml      # CI/CD
  env:
    IMAGE_NAME = diffuseproject/mdx2
    IMAGE_TAG  = <upstream>-<rev>   # e.g. 1.0.2-1
    MDX2_COMMIT = <full SHA>         # mdx2 source commit baked in

Dockerfile.notebook               # multi-stage build
  ↓ pulls notebook-env.lock        # frozen conda env (385 packages)
  ↓ git clone mdx2 @ MDX2_COMMIT   # frozen mdx2 source
  ↓ pip install -e . + jupyterhub  # editable mdx2 + pinned pip deps
  ↓ apt: openssh + sftp + rsync    # SSH gateway tooling

→ pushes to diffuseproject/mdx2:${IMAGE_TAG} on merge to main
```

Tags follow Debian-revision style: `<mdx2-version>-<our-revision>`. The trailing `-N` increments when our build changes but upstream mdx2 stays put. Tags are **immutable** — the workflow refuses to overwrite an existing tag.

## Tag scheme decision tree

```
What's changing?
├── Just `notebook-env.lock` (e.g. DIALS bump)
│     → bump trailing -N: 1.0.2-1 → 1.0.2-2
├── Just MDX2_COMMIT (mdx2 source bump, no new deps)
│     → bump upstream + reset rev: 1.0.2-1 → 1.0.3-1
├── Both (mdx2 needs new conda packages)
│     → bump upstream + reset rev: 1.0.2-1 → 1.0.3-1
└── Just OS apt packages (rare)
      → bump trailing -N: 1.0.2-1 → 1.0.2-2
```

## Upgrade scenarios

Pick the one that matches your situation.

### Scenario A: bump mdx2 source (e.g. v1.0.2 → v1.0.4)

Use this when mdx2 ships a new version and you want JupyterHub users to get it.

#### 1. Find the new MDX2_COMMIT

mdx2 currently has no git tags (as of writing), so you pin to a commit SHA. Find the latest main HEAD:

```bash
gh api /repos/diff-use/mdx2/commits/main --jq '.sha'
# → e.g. 07f074a3304baed3427a9cbf18b311f82af6af37
```

Verify the commit's `mdx2/VERSION`:

```bash
gh api '/repos/diff-use/mdx2/contents/mdx2/VERSION?ref=07f074a3304baed3427a9cbf18b311f82af6af37' --jq '.content' | base64 -d
# → 1.0.4
```

#### 2. Clean up the PR-ref scaffolding (one-time, when moving off the 1.0.2 pin)

The current Dockerfile.notebook has special-case logic to fetch `refs/pull/56/head` because the original 1.0.2 commit lives only on a deleted branch. Once you upgrade off that commit, the PR-ref fetch is unnecessary scaffolding. Remove these lines from `Dockerfile.notebook`:

```diff
 FROM debian:stable-slim AS source_stage
 ARG MDX2_COMMIT
-ARG MDX2_PR_REF=pull/56/head
 RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates git \
  && rm -rf /var/lib/apt/lists/*
 RUN git clone https://github.com/diff-use/mdx2.git /tmp/mdx2 \
  && cd /tmp/mdx2 \
- && git fetch origin "${MDX2_PR_REF}:refs/heads/source-pin" \
  && git checkout "${MDX2_COMMIT}" \
  && rm -rf .git
```

Also drop the comment block above `MDX2_PR_REF` and the build-arg in the workflow if present. The `git clone` is now sufficient because future commits live on `main` (durable).

#### 3. Update the workflow env

Edit `.github/workflows/docker.yml`:

```diff
 env:
   IMAGE_NAME: diffuseproject/mdx2
-  IMAGE_TAG: 1.0.2-1
+  IMAGE_TAG: 1.0.4-1
-  # HEAD of feat/jupyterhub-singleuser (PR #56 source ref) on 2026-02-25 — ...
-  MDX2_COMMIT: 327bf6e1541e3e0b63a22c8aff100b92c4aa6e39
+  # mdx2 main HEAD as of <DATE>, mdx2/VERSION=1.0.4 (PR #19 release merge).
+  MDX2_COMMIT: 07f074a3304baed3427a9cbf18b311f82af6af37
```

#### 4. Local build + smoke test

```bash
cd ~/ptown/mdx2-workflows/.claude/worktrees/<your-branch>

docker buildx build \
  --load \
  -f Dockerfile.notebook \
  -t mdx2-test-local:1.0.4-1 \
  --build-arg MDX2_COMMIT=07f074a3304baed3427a9cbf18b311f82af6af37 \
  .

# Smoke tests
docker run --rm mdx2-test-local:1.0.4-1 sh -c \
  '/usr/local/bin/micromamba run -n mdx2-dev python -c "import mdx2; print(mdx2.__version__)"'
# → 1.0.4

docker run --rm mdx2-test-local:1.0.4-1 sh -c 'which scp; which rsync; ls /usr/lib/openssh/sftp-server'
# → all three present

docker run --rm mdx2-test-local:1.0.4-1 sh -c \
  '/usr/local/bin/micromamba run -n mdx2-dev jupyterhub-singleuser --version'
# → 5.4.3 (or whatever pip version is pinned)
```

#### 5. Open PR, watch CI, merge

Same as the original PR #1 flow. CI on the PR will build (no push). On merge to main, the immutability check passes (1.0.4-1 doesn't exist yet), the build runs, and the image pushes to Dockerhub.

#### 6. Webapp cutover

Single-line PR against `diff-use/webapp`:

```diff
-jupyterhub_notebook_image_tag: str = "1.0.2-1"
+jupyterhub_notebook_image_tag: str = "1.0.4-1"
```

at `app/config.py:217`. Merge through normal review process.

#### 7. Verify in prod

After webapp deploys, restart the JupyterHub user notebook from the Hub Control Panel → Stop My Server → Start My Server. KubeSpawner pulls fresh because `imagePullPolicy: Always`.

> **Note on `<sampleworks-host>`**: this is the SSH endpoint of the Voltage Park machine that hosts the sampleworks k3s cluster. The actual hostname/IP is in the `diff-use/infra` Pulumi config (private repo); ask a maintainer or grep the infra repo for `sampleworks` to find it.

```bash
ssh diffuse@<sampleworks-host> \
  'sudo k3s kubectl -n jupyterhub get pod jupyter-<your-username> \
     -o jsonpath="{.spec.containers[0].image}"; echo'
# → diffuseproject/mdx2:1.0.4-1
```

### Scenario B: refresh the conda environment (DIALS bump, security patches)

Use this when mdx2 source stays the same but you want newer scientific packages — typically a quarterly cadence to avoid bit-rot in conda-forge URLs.

#### 1. Spin up a temp container with the current `env.yaml`

The cleanest way to get a fresh solve is to let micromamba resolve `env.yaml` from scratch in an isolated container:

```bash
docker run --rm -v "$PWD:/work" -w /work mambaorg/micromamba:1.5.5 bash -c '
  micromamba create -n tmp -f env.yaml --yes && \
  micromamba install -y -n tmp nexpy jupyterlab jupyterlab-h5web dials xia2 wget tar -c conda-forge && \
  micromamba env export --explicit -n tmp
' > notebook-env.lock.new
```

Then replace the old lockfile:

```bash
mv notebook-env.lock.new notebook-env.lock
```

Inspect the diff:

```bash
git diff notebook-env.lock | head -50
# Look for DIALS, dxtbx, scipy, numpy version bumps
```

Document any meaningful version moves in the PR description (especially DIALS — file format defaults can change).

#### 2. Bump the trailing revision in `IMAGE_TAG`

```diff
 env:
-  IMAGE_TAG: 1.0.2-1
+  IMAGE_TAG: 1.0.2-2
```

Don't touch `MDX2_COMMIT` for a stack-only refresh.

#### 3. Local build + smoke test + PR + cutover

Same procedure as Scenario A from step 4 onward.

#### Optional: regenerate from inside a running pod

If you'd rather pin to whatever a currently-deployed pod has installed (e.g. capturing a hand-tweaked dev pod's state):

```bash
ssh diffuse@<sampleworks-host> \
  'sudo k3s kubectl -n jupyterhub exec <pod-name> -c notebook \
     -- /usr/local/bin/micromamba env export --explicit -n mdx2-dev' \
  > notebook-env.lock
```

Then bump `IMAGE_TAG` and proceed as above.

### Scenario C: add an OS apt package

Use this when scp/sftp-server-style additions are needed but mdx2 and the conda env stay put.

#### 1. Edit `Dockerfile.notebook`

```diff
 RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       ca-certificates \
       openssh-client \
       openssh-sftp-server \
       rsync \
+      <new-package> \
  && rm -rf /var/lib/apt/lists/*
```

#### 2. Bump the trailing revision

```diff
-  IMAGE_TAG: 1.0.2-1
+  IMAGE_TAG: 1.0.2-2
```

#### 3. Local build + smoke test + PR + cutover

Same as Scenarios A and B.

## Rollback

If a `1.0.X-Y` image causes regressions in prod:

1. Revert the webapp config PR (`app/config.py:217` back to the previous tag).
2. Deploy the revert through the webapp's normal pipeline.
3. Restart any JupyterHub user pod; it pulls the previous tag.
4. The bad image stays parked on Dockerhub (immutable, harmless), but no pod uses it.

You don't need to delete or "fix" the broken image on Dockerhub. The webapp config is the source of truth for which tag prod uses; pointing it elsewhere effectively rolls back.

## Common pitfalls

### "Tag already exists on Dockerhub" error in CI

You forgot to bump `IMAGE_TAG` for a change that touched `Dockerfile.notebook` or `notebook-env.lock`. Fix: edit the workflow, bump `IMAGE_TAG`, push another commit. The check is loud-by-design — the only failure mode it allows is "you publish what you intended to publish."

### CI didn't run on a PR

The workflow has a `paths:` filter on the `push:` trigger (only `Dockerfile.notebook`, `notebook-env.lock`, and the workflow itself trigger main builds). PR builds are unfiltered. If CI didn't run on your PR but you only changed those files, check that you're working in the right repo and branch.

### Conda solve fails when regenerating the lockfile

conda-forge occasionally retires very old build artifacts. If the temp-container regeneration step fails on a missing package, the upstream `env.yaml` may need a relaxation (e.g. `python >=3.10, <3.12` instead of `<3.11`). That's an upstream mdx2 change — coordinate with mdx2 maintainers before forcing it from this repo.

### "I want the image to behave exactly like a previous version"

You can't. Conda-forge resolves freshly each time, and `git clone` of mdx2 picks up the current commit at clone time. The lockfile is the only way to get byte-equivalence with a *previously captured* environment. If you need to rebuild the *same* image bytes, pull the existing tag from Dockerhub instead of rebuilding from source.

### Webapp pod still using the old image after merge

Two checks:

1. Did the new image actually push? `curl -sS "https://hub.docker.com/v2/repositories/diffuseproject/mdx2/tags/" | jq '.results[] | {name, last_updated}'`.
2. Did KubeSpawner pull the new image? Stop the user's server from the Hub Control Panel and start it again. Default `imagePullPolicy: Always` means a fresh pull on each start.

## Reference: current pipeline state

| Item | Value | Source |
|---|---|---|
| Image registry | `diffuseproject/mdx2` | `IMAGE_NAME` in `.github/workflows/docker.yml` |
| Current tag | `1.0.2-1` | `IMAGE_TAG` in `.github/workflows/docker.yml` |
| mdx2 source pin | `327bf6e1541e3e0b63a22c8aff100b92c4aa6e39` (PR #56 head, deleted branch) | `MDX2_COMMIT` in `.github/workflows/docker.yml` |
| Conda env source | `notebook-env.lock` (385 packages, conda-forge linux-64) | repo root |
| pip pins | `jupyterhub==5.4.3`, `jupyter-vscode-proxy==0.7` | `Dockerfile.notebook` |
| Webapp consumer | `jupyterhub_notebook_image_tag` setting | `diff-use/webapp:app/config.py:217` |

## Suggested follow-ups (not required for any specific upgrade)

- **Ask mdx2 maintainers to start tagging releases.** The repo currently has zero git tags despite "v1.0.4" being a real release. Tagging would let us pin `MDX2_COMMIT: v1.0.4` instead of opaque SHAs.
- **Ask mdx2 maintainers to push the 1.0.2 source pin (`327bf6e1541e3e0b63a22c8aff100b92c4aa6e39`) as a durable git tag** (e.g. `singleuser-1.0.2-pin`). Currently we depend on `refs/pull/56/head` being kept by GitHub. The dependency disappears as soon as we upgrade off that commit, but until then a durable tag is insurance.
- **Quarterly lockfile refresh cadence.** Old conda-forge artifacts can disappear (rare, but happens). Regenerating `notebook-env.lock` every ~3 months keeps the URLs alive.
