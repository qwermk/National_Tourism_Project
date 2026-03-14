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
    year,
    sum(total_visitors) as total_visitors,
    round(sum(total_spending_usd), 0) as total_spending_usd
from tourism.fct_tourism_arrivals
group by year
order by year
```

<BarChart
    data={arrivals_by_year}
    x=year
    y=total_visitors
    title="Total de Visitantes por Año"
/>

<LineChart
    data={arrivals_by_year}
    x=year
    y=total_spending_usd
    title="Gasto Total (USD) por Año"
    yFmt=usd
/>

---

## Top 10 Países de Origen

```sql top_countries
select
    country_of_origin,
    sum(total_visitors) as total_visitors,
    round(avg(average_spending_usd), 2) as average_spending_usd
from tourism.fct_tourism_arrivals
group by country_of_origin
order by total_visitors desc
limit 10
```

<DataTable data={top_countries} />

<BarChart
    data={top_countries}
    x=country_of_origin
    y=total_visitors
    title="Principales Países de Origen"
    swapXY=true
/>

---

## Ocupación Hotelera por Departamento

```sql occupancy_trend
select
    year,
    month,
    department,
    occupancy_rate
from tourism.fct_hotel_occupancy
order by year, month
```

<LineChart
    data={occupancy_trend}
    x=month
    y=occupancy_rate
    series=department
    title="Tendencia de Ocupación Hotelera Mensual"
    yFmt=pct
/>

---

## Estacionalidad del Turismo

```sql seasonality
select
    month,
    sum(total_visitors) as total_visitors,
    round(avg(average_spending_usd), 2) as average_spending
from tourism.fct_tourism_arrivals
group by month
order by month
```

<BarChart
    data={seasonality}
    x=month
    y=total_visitors
    title="Estacionalidad — Visitantes por Mes"
/>

---

<Alert status="info">
    Datos actualizados diariamente vía pipeline Dagster.
    Fuentes: CITUR, DANE, Migración Colombia.
</Alert>
