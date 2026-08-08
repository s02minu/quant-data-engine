FROM python:3.12-slim

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
