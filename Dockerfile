# Default mode: pull pre-built mdx2 image from Docker Hub.
# Fast — no source build, just adds Prefect on top.
#
# TODO(mdx2-versioning): Available tags at:
# https://hub.docker.com/repository/docker/diffuseproject/mdx2/tags
# TODO(mdx2-releases): Pin to a release tag once GitHub releases exist:
# https://github.com/diff-use/mdx2/releases
ARG MDX2_IMAGE_TAG=1.0.0
FROM diffuseproject/mdx2:${MDX2_IMAGE_TAG}

WORKDIR /home/dev

COPY pyproject.toml .
COPY mdx2_workflows/ mdx2_workflows/
COPY prefect/ prefect/

RUN micromamba run -n mdx2-dev pip install --no-cache-dir prefect && \
    micromamba run -n mdx2-dev pip install --no-cache-dir .
