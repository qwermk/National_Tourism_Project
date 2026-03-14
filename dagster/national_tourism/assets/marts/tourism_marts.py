# =============================================================================
# Gold Layer — Modelos de negocio (Marts) — ARCHIVO LEGADO
# =============================================================================
#
# ⚠️  NOTA IMPORTANTE PARA EL EQUIPO:
#
#   Este archivo ya NO se usa en el pipeline activo.
#   Fue reemplazado por los modelos dbt SQL en:
#     dbt/models/marts/fct_tourism_arrivals.sql
#     dbt/models/marts/fct_hotel_occupancy.sql
#     dbt/models/marts/fct_tourism_gdp.sql
#     dbt/models/marts/fct_migration_flows.sql
#     dbt/models/marts/dim_departments.sql
#     dbt/models/marts/dim_date.sql
#
#   El pipeline actual funciona así:
#     1. Python (ingestion/) → sube datos crudos a MinIO (Bronze)
#     2. dbt (dbt/models/)   → transforma datos en DuckDB (Silver → Gold)
#
#   Se conserva este archivo como referencia educativa.
#   NO hacer import de este módulo en definitions.py ni en __init__.py.
# =============================================================================

# El código original se conserva comentado como referencia educativa.
# Para ver la implementación activa, revisa los modelos dbt en:
#   dbt/models/marts/
