-- =============================================================================
-- fct_air_passengers.sql — Fact: Pasajeros Aéreos (Gold)
-- =============================================================================
-- Tráfico aéreo mensual por aeropuerto en Colombia — Aerocivil.
-- Granularidad: mes × aeropuerto
-- =============================================================================

with passengers as (

    select * from {{ ref('stg_aerocivil_passengers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year', 'month', 'airport'
        ]) }}                                   as passenger_id,

        year,
        month,
        period_date,
        airport,
        airport_city,
        domestic_passengers,
        international_passengers,
        total_passengers,

        -- Porcentaje internacional
        case
            when total_passengers > 0
            then round(
                (international_passengers::double / total_passengers) * 100, 1
            )
            else 0
        end                                     as pct_international,

        current_timestamp                        as loaded_at

    from passengers

)

select * from final
