-- =============================================================================
-- stg_hotel_occupancy.sql — Limpieza de ocupación hotelera (Silver)
-- =============================================================================

with source as (

    select * from {{ source('bronze', 'hotel_occupancy') }}

),

cleaned as (

    select
        -- Periodo
        cast(year as integer)                   as year,
        cast(month as integer)                  as month,
        make_date(year, month, 1)               as period_date,

        -- Dimensiones
        trim({{ initcap('department') }})               as department,

        -- Métricas (validadas)
        least(greatest(cast(occupancy_rate as double), 0), 100)
                                                as occupancy_rate,
        coalesce(cast(available_rooms as integer), 0)
                                                as available_rooms,
        coalesce(cast(occupied_rooms as integer), 0)
                                                as occupied_rooms,
        coalesce(cast(average_rate_cop as double), 0)
                                                as average_rate_cop

    from source
    where
        year >= 2015
        and month between 1 and 12

)

select * from cleaned
