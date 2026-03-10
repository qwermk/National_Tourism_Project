---
title: Análisis por Departamento
description: Detalle de turismo y ocupación por departamento colombiano
---

# Análisis por Departamento

```sql departments
select distinct departamento_destino as departamento
from gold.fct_tourism_arrivals
order by departamento
```

<Dropdown name=selected_dept data={departments} value=departamento title="Selecciona un departamento" />

---

## Visitantes en {inputs.selected_dept}

```sql dept_arrivals
select
    anio,
    mes,
    sum(total_visitantes) as total_visitantes,
    round(sum(gasto_total_usd), 0) as gasto_total_usd
from gold.fct_tourism_arrivals
where departamento_destino = '${inputs.selected_dept}'
group by anio, mes
order by anio, mes
```

<LineChart
    data={dept_arrivals}
    x=mes
    y=total_visitantes
    series=anio
    title="Visitantes por Mes en {inputs.selected_dept}"
/>

---

## Ocupación Hotelera en {inputs.selected_dept}

```sql dept_occupancy
select
    anio,
    mes,
    avg_ocupacion,
    tarifa_promedio_cop
from gold.fct_hotel_occupancy
where departamento = '${inputs.selected_dept}'
order by anio, mes
```

<LineChart
    data={dept_occupancy}
    x=mes
    y=avg_ocupacion
    series=anio
    title="Ocupación Hotelera (%) en {inputs.selected_dept}"
    yFmt=pct
/>

<DataTable data={dept_occupancy} />
