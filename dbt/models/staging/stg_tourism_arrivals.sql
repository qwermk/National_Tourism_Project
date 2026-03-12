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
        cast(fecha_llegada as date)         as fecha_llegada,
        cast(anio as integer)               as anio,
        cast(mes as integer)                as mes,

        -- Dimensiones (estandarizadas)
        trim({{ initcap('pais_origen') }})          as pais_origen,
        trim({{ initcap('departamento_destino') }}) as departamento_destino,
        trim({{ initcap('motivo_viaje') }})         as motivo_viaje,
        trim({{ initcap('punto_entrada') }})        as punto_entrada,

        -- Métricas
        coalesce(cast(numero_visitantes as integer), 0)  as numero_visitantes,
        coalesce(cast(gasto_estimado_usd as double), 0)  as gasto_estimado_usd

    from source
    where
        -- Filtrar registros inválidos
        numero_visitantes >= 0
        and anio >= 2015
        and mes between 1 and 12

)

select * from cleaned
