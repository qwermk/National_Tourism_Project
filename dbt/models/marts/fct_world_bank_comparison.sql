-- =============================================================================
-- fct_world_bank_comparison.sql — Fact: Comparación Regional (Gold)
-- =============================================================================
-- Indicadores turísticos de Colombia vs. países vecinos (World Bank).
-- Granularidad: año × país × indicador
-- =============================================================================

with regional as (

    select * from {{ ref('stg_world_bank_regional') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'year', 'country_code', 'indicator_code'
        ]) }}                                   as comparison_id,

        year,
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        value,
        source,
        current_timestamp                        as loaded_at

    from regional

)

select * from final
