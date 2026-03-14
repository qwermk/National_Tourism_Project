# =============================================================================
# Silver Layer — Staging de datos de turismo (ARCHIVO LEGADO)
# =============================================================================
#
# ⚠️  NOTA IMPORTANTE PARA EL EQUIPO:
#
#   Este archivo ya NO se usa en el pipeline activo.
#   Fue reemplazado por los modelos dbt SQL en:
#     dbt/models/staging/stg_tourism_arrivals.sql
#     dbt/models/staging/stg_hotel_occupancy.sql
#
#   El pipeline actual funciona así:
#     1. Python (ingestion/) → sube datos crudos a MinIO (Bronze)
#     2. dbt (dbt/models/)   → transforma datos en DuckDB (Silver → Gold)
#
#   Se conserva este archivo como referencia educativa de cómo se hacía
#   el staging en Python antes de migrar a dbt.
#
#   NO hacer import de este módulo en definitions.py ni en __init__.py.
# =============================================================================

# El código original se conserva comentado como referencia educativa.
# Para ver la implementación activa, revisa:
#   dbt/models/staging/stg_tourism_arrivals.sql
#   dbt/models/staging/stg_hotel_occupancy.sql
