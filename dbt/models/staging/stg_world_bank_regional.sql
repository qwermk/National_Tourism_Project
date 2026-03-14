-- =============================================================================
-- stg_world_bank_regional.sql — Comparación Regional World Bank (Silver)
-- =============================================================================
-- Indicadores de turismo para Colombia y países vecinos.
-- =============================================================================

with source as (

    select * from {{ source('world_bank', 'regional_comparison') }}

),

cleaned as (

    select
        cast(year as integer)                    as year,
        trim(country_code)                       as country_code,
        trim(country_name)                       as country_name,
        trim(indicator_code)                     as indicator_code,
        trim(indicator_name)                     as indicator_name,
        coalesce(cast(value as double), 0.0)     as value,
        coalesce(trim(source), 'world_bank')     as source

    from source
    where
        year >= 2010
        and value > 0

)

select * from cleaned
