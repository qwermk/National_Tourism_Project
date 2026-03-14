-- =============================================================================
-- stg_aerocivil_passengers.sql — Pasajeros aéreos Aerocivil (Silver)
-- =============================================================================
-- Limpieza de datos de pasajeros aéreos por aeropuerto de Colombia.
-- =============================================================================

with source as (

    select * from {{ source('aerocivil', 'passengers') }}

),

cleaned as (

    select
        -- Periodo
        cast(year as integer)                                       as year,
        cast(month as integer)                                      as month,
        make_date(year, month, 1)                                   as period_date,

        -- Dimensiones
        trim(airport)                                               as airport,
        trim(airport_city)                                          as airport_city,

        -- Métricas
        coalesce(cast(domestic_passengers as integer), 0)           as domestic_passengers,
        coalesce(cast(international_passengers as integer), 0)      as international_passengers,
        coalesce(cast(total_passengers as integer), 0)              as total_passengers

    from source
    where
        year >= 2010
        and month between 1 and 12
        and total_passengers > 0

)

select * from cleaned
