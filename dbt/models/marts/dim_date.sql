-- =============================================================================
-- dim_date.sql — Dimensión de fechas (Gold)
-- =============================================================================
-- Tabla de fechas para análisis de series temporales.
-- Permite detectar gaps y hacer joins con fact tables por periodo.
-- Rango: 2019-01-01 → 2024-12-31 (configurable via vars en dbt_project.yml)
-- =============================================================================

with dates as (

    {{ generate_date_spine(var('start_date'), var('end_date')) }}

),

final as (

    select
        -- Clave primaria
        date_day,

        -- Atributos básicos
        cast(anio as integer)                                           as anio,
        cast(mes as integer)                                            as mes,
        cast(dia as integer)                                            as dia,
        cast(dia_semana as integer)                                     as dia_semana,
        trimestre,

        -- Período mensual (para JOIN con fct_tourism_arrivals)
        make_date(cast(anio as integer), cast(mes as integer), 1)       as fecha_periodo,

        -- Nombres legibles
        case cast(dia_semana as integer)
            when 0 then 'Domingo'
            when 1 then 'Lunes'
            when 2 then 'Martes'
            when 3 then 'Miércoles'
            when 4 then 'Jueves'
            when 5 then 'Viernes'
            when 6 then 'Sábado'
        end                                                             as nombre_dia,

        case cast(mes as integer)
            when 1  then 'Enero'
            when 2  then 'Febrero'
            when 3  then 'Marzo'
            when 4  then 'Abril'
            when 5  then 'Mayo'
            when 6  then 'Junio'
            when 7  then 'Julio'
            when 8  then 'Agosto'
            when 9  then 'Septiembre'
            when 10 then 'Octubre'
            when 11 then 'Noviembre'
            when 12 then 'Diciembre'
        end                                                             as nombre_mes,

        -- Indicadores
        case
            when cast(dia_semana as integer) in (0, 6) then true
            else false
        end                                                             as es_fin_de_semana,

        current_timestamp                                               as loaded_at

    from dates

)

select * from final
