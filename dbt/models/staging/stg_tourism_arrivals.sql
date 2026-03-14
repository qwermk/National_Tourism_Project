-- =============================================================================
-- stg_tourism_arrivals.sql — Limpieza de llegadas de turistas (Silver)
-- =============================================================================
-- Fuente: Bronze layer (MinIO Parquet via DuckDB S3)
-- Transformaciones:
--   - Casteo de tipos
--   - Estandarización de texto
--   - Eliminación de registros inválidos
-- =============================================================================

with source as (

    select * from {{ source('bronze', 'tourism_arrivals') }}

),

cleaned as (

    select
        -- Fechas
        cast(arrival_date as date)         as arrival_date,
        cast(year as integer)              as year,
        cast(month as integer)             as month,

        -- Dimensiones (estandarizadas)
        trim({{ initcap('country_of_origin') }})          as country_of_origin,
        trim({{ initcap('destination_department') }}) as destination_department,
        trim({{ initcap('travel_purpose') }})         as travel_purpose,
        trim({{ initcap('entry_point') }})        as entry_point,

        -- Métricas
        coalesce(cast(number_of_visitors as integer), 0)  as number_of_visitors,
        coalesce(cast(estimated_spending_usd as double), 0)  as estimated_spending_usd

    from source
    where
        -- Filtrar registros inválidos
        number_of_visitors >= 0
        and year >= 2015
        and month between 1 and 12

)

select * from cleaned
