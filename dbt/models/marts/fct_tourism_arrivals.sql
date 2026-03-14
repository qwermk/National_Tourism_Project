-- =============================================================================
-- fct_tourism_arrivals.sql — Fact: Llegadas de turistas (Gold / Mart)
-- =============================================================================
-- Tabla de hechos principal para análisis de turismo receptivo.
-- Granularidad: mes × país × departamento × motivo × punto de entrada
-- =============================================================================

with monthly as (

    select * from {{ ref('int_arrivals_monthly') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'year', 'month', 'country_of_origin',
            'destination_department', 'travel_purpose', 'entry_point'
        ]) }}                                   as arrival_id,

        -- Dimensiones
        year,
        month,
        make_date(year, month, 1)               as period_date,
        country_of_origin,
        destination_department,
        travel_purpose,
        entry_point,

        -- Métricas
        total_visitors,
        round(total_spending_usd, 2)             as total_spending_usd,
        round(average_spending_usd, 2)           as average_spending_usd,
        num_records,

        -- Metadata
        current_timestamp                        as loaded_at

    from monthly

)

select * from final
