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
        cast(year as integer)                                           as year,
        cast(month as integer)                                          as month,
        cast(day as integer)                                            as day,
        cast(day_of_week as integer)                                    as day_of_week,
        quarter,

        -- Período mensual (para JOIN con fct_tourism_arrivals)
        make_date(cast(year as integer), cast(month as integer), 1)     as period_date,

        -- Nombres legibles
        case cast(day_of_week as integer)
            when 0 then 'Domingo'
            when 1 then 'Lunes'
            when 2 then 'Martes'
            when 3 then 'Miércoles'
            when 4 then 'Jueves'
            when 5 then 'Viernes'
            when 6 then 'Sábado'
        end                                                             as day_name,

        case cast(month as integer)
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
        end                                                             as month_name,

        -- Indicadores
        case
            when cast(day_of_week as integer) in (0, 6) then true
            else false
        end                                                             as is_weekend,

        current_timestamp                                               as loaded_at

    from dates

)

select * from final
