-- =============================================================================
-- stg_migracion_flows.sql — Flujos Migratorios de Colombia (Silver)
-- =============================================================================
-- ¿Qué hace?
--   Limpia y estandariza los datos de Migración Colombia sobre
--   entradas y salidas de viajeros internacionales.
--
-- ¿De dónde vienen los datos?
--   Tabla bronze: migracion_flows (archivo Parquet en MinIO)
--
-- ¿A dónde van?
--   Vista en DuckDB → schema staging
-- =============================================================================

with source as (

    select * from {{ source('migracion', 'flows') }}

),

cleaned as (

    select
        -- Periodo
        cast(year as integer) as year,
        cast(month as integer)  as month,
        make_date(year, month, 1) as period_date,

        -- Dimensiones
        trim(nationality)                                   as nationality,
        lower(trim(movement_type))                          as movement_type,
        trim(control_point)                                 as control_point,

        -- Métricas
        coalesce(cast(number_of_travelers as integer), 0)   as number_of_travelers,

        -- Metadata
        coalesce(trim(source), 'migracion_colombia')        as source

    from source
    where
        year >= 2015
        and month between 1 and 12
        and number_of_travelers > 0

)

select * from cleaned
