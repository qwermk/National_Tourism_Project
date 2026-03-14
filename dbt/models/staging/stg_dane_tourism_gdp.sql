-- =============================================================================
-- stg_dane_tourism_gdp.sql — PIB Turístico de Colombia (Silver)
-- =============================================================================
-- ¿Qué hace?
--   Limpia y valida los datos del DANE sobre la contribución del turismo
--   al PIB de Colombia.
--
-- ¿De dónde vienen los datos?
--   Tabla bronze: dane_tourism_gdp (archivo Parquet en MinIO)
--
-- ¿A dónde van?
--   Vista en DuckDB → schema staging
-- =============================================================================

with source as (

    select * from {{ source('dane', 'tourism_gdp') }}

),

cleaned as (

    select
        -- Periodo
        cast(year as integer) as year,

        -- Métricas económicas
        coalesce(cast(tourism_gdp_billions_cop as double), 0.0) as tourism_gdp_billions_cop,
        coalesce(cast(pct_of_total_gdp as double), 0.0)         as pct_of_total_gdp,
        coalesce(cast(tourism_employment_thousands as integer), 0) as tourism_employment_thousands,
        coalesce(cast(annual_variation_pct as double), 0.0)     as annual_variation_pct,

        -- Metadata
        coalesce(trim(source), 'dane') as source

    from source
    where
        year >= 2010
        and tourism_gdp_billions_cop > 0

)

select * from cleaned
