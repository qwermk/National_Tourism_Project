-- =============================================================================
-- dim_departments.sql — Dimensión: Departamentos de Colombia (Gold)
-- =============================================================================
-- Catálogo estático de departamentos con metadata geográfica.
-- Se carga desde un seed CSV.
-- =============================================================================

with departments as (

    select * from {{ ref('seed_departments') }}

),

final as (

    select
        dane_code,
        department,
        capital,
        region,
        current_timestamp as loaded_at

    from departments

)

select * from final
