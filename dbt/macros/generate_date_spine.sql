-- =============================================================================
-- Macro: generate_date_spine — Genera una tabla de fechas
-- =============================================================================
-- Útil para análisis de series temporales y detectar gaps.
-- =============================================================================

{% macro generate_date_spine(start_date, end_date) %}

    with date_spine as (
        select
            unnest(
                generate_series(
                    date '{{ start_date }}',
                    date '{{ end_date }}',
                    interval '1 day'
                )
            ) as date_day
    )

    select
        date_day,
        extract(year from date_day)     as anio,
        extract(month from date_day)    as mes,
        extract(day from date_day)      as dia,
        extract(dow from date_day)      as dia_semana,
        case
            when extract(month from date_day) in (12, 1, 2) then 'Q1'
            when extract(month from date_day) in (3, 4, 5) then 'Q2'
            when extract(month from date_day) in (6, 7, 8) then 'Q3'
            else 'Q4'
        end as trimestre

    from date_spine

{% endmacro %}
