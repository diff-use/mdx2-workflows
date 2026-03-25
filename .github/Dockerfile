# diffuseproject/mdx2:1.0.0 (https://hub.docker.com/repository/docker/diffuseproject/mdx2/tags/1.0.0/sha256-abfcf7d2feacdf9a207a13c2bb908d62e6f461259f78e82d3bb8983267446291)

FROM mambaorg/micromamba:1.5.5 AS micromamba_stage

FROM python:3.10-slim AS final

RUN apt-get update && apt-get install -y ca-certificates git

COPY --from=micromamba_stage /bin/micromamba /usr/local/bin/micromamba

ENV PATH="/usr/local/bin:/opt/conda/bin:$PATH"
ENV JUPYTER_PORT=8888

RUN mkdir -p /opt/conda

WORKDIR /home/dev

COPY env.yaml .

# Use micromamba to create the environment and install packages
RUN /usr/local/bin/micromamba create -f env.yaml -n mdx2-dev && \
    /usr/local/bin/micromamba install -y -n mdx2-dev nexpy jupyterlab jupyterlab-h5web dials xia2 wget tar -c conda-forge && \
    /usr/local/bin/micromamba clean --all --yes

COPY . .

# Install dials-extensions pinned to a specific commit (963fe9e458a505a8443c988d45fb8dcb4532768f) for CHESS beamline compatibility
# Install the local package in editable mode within the environment
RUN /usr/local/bin/micromamba run -n mdx2-dev pip install git+https://github.com/FlexXBeamline/dials-extensions.git@963fe9e458a505a8443c988d45fb8dcb4532768f
RUN /usr/local/bin/micromamba run -n mdx2-dev pip install -e .

EXPOSE 8880-8890

CMD /usr/local/bin/micromamba run -n mdx2-dev jupyter lab --ip=0.0.0.0 --port=$JUPYTER_PORT --no-browser --allow-root
