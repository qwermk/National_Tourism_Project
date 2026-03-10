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
        anio,
        mes,
        pais_origen,
        departamento_destino,
        motivo_viaje,
        punto_entrada,

        -- Métricas agregadas
        sum(numero_visitantes)          as total_visitantes,
        sum(gasto_estimado_usd)         as gasto_total_usd,
        avg(gasto_estimado_usd)         as gasto_promedio_usd,
        count(*)                        as num_registros

    from arrivals
    group by
        anio, mes, pais_origen, departamento_destino,
        motivo_viaje, punto_entrada

)

select * from monthly_agg
