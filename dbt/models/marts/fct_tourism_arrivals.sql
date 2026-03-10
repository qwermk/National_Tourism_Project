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
            'anio', 'mes', 'pais_origen',
            'departamento_destino', 'motivo_viaje', 'punto_entrada'
        ]) }}                                   as arrival_id,

        -- Dimensiones
        anio,
        mes,
        make_date(anio, mes, 1)                 as fecha_periodo,
        pais_origen,
        departamento_destino,
        motivo_viaje,
        punto_entrada,

        -- Métricas
        total_visitantes,
        round(gasto_total_usd, 2)               as gasto_total_usd,
        round(gasto_promedio_usd, 2)             as gasto_promedio_usd,
        num_registros,

        -- Metadata
        current_timestamp                        as loaded_at

    from monthly

)

select * from final
