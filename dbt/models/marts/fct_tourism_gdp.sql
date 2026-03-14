-- =============================================================================
-- fct_tourism_gdp.sql — Fact: PIB Turístico de Colombia (Gold / Mart)
-- =============================================================================
-- ¿Qué hace?
--   Tabla de hechos con indicadores económicos del turismo colombiano:
--   PIB turístico, porcentaje del PIB total, empleo y crecimiento anual.
--
-- Granularidad: anual (un registro por año)
-- =============================================================================

with gdp as (

    select * from {{ ref('stg_dane_tourism_gdp') }}

),

final as (

    select
        -- Clave única
        {{ dbt_utils.generate_surrogate_key(['year']) }} as gdp_id,

        -- Dimensiones
        year,

        -- Métricas económicas
        tourism_gdp_billions_cop,
        pct_of_total_gdp,
        tourism_employment_thousands,
        annual_variation_pct,

        -- Metadata
        source,
        current_timestamp as loaded_at

    from gdp

)

select * from final
