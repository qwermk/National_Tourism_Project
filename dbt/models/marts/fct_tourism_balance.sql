-- =============================================================================
-- fct_tourism_balance.sql — Fact: Balanza Turística (Gold)
-- =============================================================================
-- Balanza de pagos turística de Colombia — Banco de la República.
-- Granularidad: año × trimestre
-- =============================================================================

with balance as (

    select * from {{ ref('stg_banrep_tourism_balance') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year', 'quarter'
        ]) }}                                                   as balance_id,

        year,
        quarter,
        tourism_income_usd_millions,
        tourism_expenditure_usd_millions,
        tourism_balance_usd_millions,

        -- Balance acumulado anual (window function)
        sum(tourism_income_usd_millions) over (
            partition by year order by quarter
        )                                                       as ytd_income,
        sum(tourism_expenditure_usd_millions) over (
            partition by year order by quarter
        )                                                       as ytd_expenditure,

        source,
        current_timestamp                                        as loaded_at

    from balance

)

select * from final
