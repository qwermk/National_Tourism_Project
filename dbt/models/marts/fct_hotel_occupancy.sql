-- =============================================================================
-- fct_hotel_occupancy.sql — Fact: Ocupación hotelera (Gold / Mart)
-- =============================================================================
-- Tabla de hechos de ocupación hotelera por departamento y periodo.
-- =============================================================================

with occupancy as (

    select * from {{ ref('stg_hotel_occupancy') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'anio', 'mes', 'departamento'
        ]) }}                                   as occupancy_id,

        -- Dimensiones
        anio,
        mes,
        fecha_periodo,
        departamento,

        -- Métricas
        porcentaje_ocupacion,
        habitaciones_disponibles,
        habitaciones_ocupadas,
        round(tarifa_promedio_cop, 0)           as tarifa_promedio_cop,

        -- Métricas calculadas
        case
            when habitaciones_disponibles > 0
            then round(
                (habitaciones_ocupadas::double / habitaciones_disponibles) * 100, 1
            )
            else 0
        end                                     as ocupacion_calculada,

        -- Metadata
        current_timestamp                        as loaded_at

    from occupancy

)

select * from final
