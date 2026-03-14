-- =============================================================================
-- fct_migration_flows.sql — Fact: Flujos Migratorios (Gold / Mart)
-- =============================================================================
-- ¿Qué hace?
--   Tabla de hechos con el total de entradas y salidas de viajeros
--   internacionales a Colombia, agregado por mes y nacionalidad.
--
-- Granularidad: month × nationality × movement_type × control_point
-- =============================================================================

with migration as (

    select * from {{ ref('stg_migracion_flows') }}

),

final as (

    select
        -- Clave única (surrogate key)
        {{ dbt_utils.generate_surrogate_key([
            'year', 'month', 'nationality', 'movement_type', 'control_point'
        ]) }} as migration_id,

        -- Dimensiones
        year,
        month,
        period_date,
        nationality,
        movement_type,
        control_point,

        -- Métricas
        number_of_travelers,

        -- Metadata
        current_timestamp as loaded_at

    from migration

)

select * from final
