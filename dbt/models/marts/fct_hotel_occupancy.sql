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
            'year', 'month', 'department'
        ]) }}                                   as occupancy_id,

        -- Dimensiones
        year,
        month,
        period_date,
        department,

        -- Métricas
        occupancy_rate,
        available_rooms,
        occupied_rooms,
        round(average_rate_cop, 0)              as average_rate_cop,

        -- Métricas calculadas
        case
            when available_rooms > 0
            then round(
                (occupied_rooms::double / available_rooms) * 100, 1
            )
            else 0
        end                                     as calculated_occupancy,

        -- Metadata
        current_timestamp                        as loaded_at

    from occupancy

)

select * from final
