-- =============================================================================
-- stg_world_bank_arrivals.sql — Totales anuales World Bank (Silver)
-- =============================================================================
-- Fuente: bronze.world_bank.arrivals_annual (asset raw_world_bank_arrivals)
-- Uso: referencia para validación cruzada con datos CITUR/sintéticos.
--      Proporciona totales anuales de llegadas internacionales a Colombia.
-- =============================================================================

with source as (

    select * from {{ source('world_bank', 'arrivals_annual') }}

),

cleaned as (

    select
        cast(anio                as integer)  as anio,
        trim(indicador_codigo)               as indicador_codigo,
        trim(indicador_nombre)               as indicador_nombre,
        coalesce(cast(valor as double), 0.0) as valor,
        trim(pais_codigo)                    as pais_codigo,
        coalesce(trim(fuente), 'world_bank') as fuente

    from source
    where
        anio >= 2010
        and valor > 0

)

select * from cleaned
