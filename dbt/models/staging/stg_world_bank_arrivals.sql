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
        cast(year                as integer)  as year,
        trim(indicator_code)                 as indicator_code,
        trim(indicator_name)                 as indicator_name,
        coalesce(cast(value as double), 0.0) as value,
        trim(country_code)                   as country_code,
        coalesce(trim(source), 'world_bank') as source

    from source
    where
        year >= 2010
        and value > 0

)

select * from cleaned
