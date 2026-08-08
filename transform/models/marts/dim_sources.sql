-- Gold: the source catalogue (ROADMAP 3.1's third consumer of the registry -- the
-- same SourceSpecs that configure ingestors and set DQ thresholds also *are* the
-- published catalogue). The seed is regenerated from qde.registry.dim_sources()
-- before each build (see scripts/maintain.sh); this materializes it as a gold
-- Parquet file alongside the bar marts, so a client can discover what exists,
-- what's fresh, and what's redistributable straight from the lake.
{{ config(
    materialized='external',
    location=var('lake_root') ~ '/gold/dim_sources/data.parquet',
    format='parquet'
) }}

select * from {{ ref('dim_sources_seed') }}
