-- =============================================================================
-- int_arrivals_monthly.sql — Agregación mensual de llegadas (Intermediate)
-- =============================================================================
-- Modelo efímero que agrega las llegadas por mes para alimentar los marts.
-- =============================================================================

with arrivals as (

    select * from {{ ref('stg_tourism_arrivals') }}

),

monthly_agg as (

    select
        year,
        month,
        country_of_origin,
        destination_department,
        travel_purpose,
        entry_point,

        -- Métricas agregadas
        sum(number_of_visitors)          as total_visitors,
        sum(estimated_spending_usd)      as total_spending_usd,
        avg(estimated_spending_usd)      as average_spending_usd,
        count(*)                         as num_records

    from arrivals
    group by
        year, month, country_of_origin, destination_department,
        travel_purpose, entry_point

)

select * from monthly_agg
