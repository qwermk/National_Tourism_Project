---
title: Análisis por Departamento
description: Detalle de turismo y ocupación por departamento colombiano
---


```sql departments
select distinct destination_department as department
from tourism.fct_tourism_arrivals
order by department
```

<Dropdown name=selected_dept data={departments} value=department title="Selecciona un departamento" />

---

## Visitantes en {inputs.selected_dept}

```sql dept_arrivals
select
    year,
    month,
    sum(total_visitors) as total_visitors,
    round(sum(total_spending_usd), 0) as total_spending_usd
from tourism.fct_tourism_arrivals
where destination_department = '${inputs.selected_dept}'
group by year, month
order by year, month
```

<LineChart
    data={dept_arrivals}
    x=month
    y=total_visitors
    series=year
    title="Visitantes por Mes en {inputs.selected_dept}"
/>

---

## Ocupación Hotelera en {inputs.selected_dept}

```sql dept_occupancy
select
    year,
    month,
    occupancy_rate,
    average_rate_cop
from tourism.fct_hotel_occupancy
where department = '${inputs.selected_dept}'
order by year, month
```

<LineChart
    data={dept_occupancy}
    x=month
    y=occupancy_rate
    series=year
    title="Ocupación Hotelera (%) en {inputs.selected_dept}"
    yFmt=pct
/>

<DataTable data={dept_occupancy} />
