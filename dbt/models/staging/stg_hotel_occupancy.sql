-- =============================================================================
-- stg_hotel_occupancy.sql — Limpieza de ocupación hotelera (Silver)
-- =============================================================================

with source as (

    select * from {{ source('bronze', 'hotel_occupancy') }}

),

cleaned as (

    select
        -- Periodo
        cast(anio as integer)                   as anio,
        cast(mes as integer)                    as mes,
        make_date(anio, mes, 1)                 as fecha_periodo,

        -- Dimensiones
        trim({{ initcap('departamento') }})             as departamento,

        -- Métricas (validadas)
        least(greatest(cast(porcentaje_ocupacion as double), 0), 100)
                                                as porcentaje_ocupacion,
        coalesce(cast(habitaciones_disponibles as integer), 0)
                                                as habitaciones_disponibles,
        coalesce(cast(habitaciones_ocupadas as integer), 0)
                                                as habitaciones_ocupadas,
        coalesce(cast(tarifa_promedio_cop as double), 0)
                                                as tarifa_promedio_cop

    from source
    where
        anio >= 2015
        and mes between 1 and 12

)

select * from cleaned
