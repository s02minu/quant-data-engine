# Pinned by DIGEST, not by tag. `python:3.12-slim` is mutable — it moves with every
# patch release — so two builds of the same commit could ship different runtimes,
# which is precisely the drift that makes an unattended collector hard to reason
# about. The digest below is the image running in production.
#
# To bump: `docker pull python:3.12-slim`, take the new digest from
# `docker image inspect python:3.12-slim --format '{{index .RepoDigests 0}}'`,
# rebuild, and run the suite before deploying.
FROM python@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
# The dbt project, so the nightly `docker compose run collector ... dbt build` can
# rebuild gold from the mounted /data lake.
COPY transform ./transform

# `.[transform]` pulls dbt-core + dbt-duckdb so the batch container can run dbt.
RUN pip install --no-cache-dir ".[transform]"

CMD ["python", "-m", "qde.stream"]
