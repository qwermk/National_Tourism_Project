---
title: Turismo Colombia — Dashboard Principal
description: Análisis del turismo nacional e internacional en Colombia
---

# 🇨🇴 Turismo en Colombia

Dashboard interactivo que analiza las principales métricas de turismo en Colombia,
alimentado por el pipeline de datos **Dagster + dbt + DuckDB**.

---

## Llegadas de Turistas Internacionales

```sql arrivals_by_year
select
    anio,
    sum(total_visitantes) as total_visitantes,
    round(sum(gasto_total_usd), 0) as gasto_total_usd
from gold.fct_tourism_arrivals
group by anio
order by anio
```

<BarChart
    data={arrivals_by_year}
    x=anio
    y=total_visitantes
    title="Total de Visitantes por Año"
/>

<LineChart
    data={arrivals_by_year}
    x=anio
    y=gasto_total_usd
    title="Gasto Total (USD) por Año"
    yFmt=usd
/>

---

## Top 10 Países de Origen

```sql top_countries
select
    pais_origen,
    sum(total_visitantes) as total_visitantes,
    round(avg(gasto_promedio_usd), 2) as gasto_promedio_usd
from gold.fct_tourism_arrivals
group by pais_origen
order by total_visitantes desc
limit 10
```

<DataTable data={top_countries} />

<BarChart
    data={top_countries}
    x=pais_origen
    y=total_visitantes
    title="Principales Países de Origen"
    swapXY=true
/>

---

## Ocupación Hotelera por Departamento

```sql occupancy_trend
select
    anio,
    mes,
    departamento,
    avg_ocupacion
from gold.fct_hotel_occupancy
order by anio, mes
```

<LineChart
    data={occupancy_trend}
    x=mes
    y=avg_ocupacion
    series=departamento
    title="Tendencia de Ocupación Hotelera Mensual"
    yFmt=pct
/>

---

## Estacionalidad del Turismo

```sql seasonality
select
    mes,
    sum(total_visitantes) as total_visitantes,
    round(avg(gasto_promedio_usd), 2) as gasto_promedio
from gold.fct_tourism_arrivals
group by mes
order by mes
```

<BarChart
    data={seasonality}
    x=mes
    y=total_visitantes
    title="Estacionalidad — Visitantes por Mes"
/>

---

<Alert status="info">
    Datos actualizados diariamente vía pipeline Dagster.
    Fuentes: CITUR, DANE, Migración Colombia.
</Alert>
