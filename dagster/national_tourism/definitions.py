# =============================================================================
# Dagster Definitions — Punto de entrada principal
# =============================================================================
# Pipeline unificado Dagster ↔ dbt (sin duplicación de lógica):
#
#   ingestion assets (Python)   →  Bronze en MinIO
#   @dbt_assets (dbt build)     →  Silver views + Gold tables en DuckDB
#
# Los assets Python de staging y marts han sido reemplazados por dbt.
# Ver dagster/national_tourism/assets/dbt_assets.py para el @dbt_assets.
# =============================================================================

from dagster import Definitions, load_assets_from_package_module

from national_tourism.assets import ingestion
from national_tourism.assets.dbt_assets import national_tourism_dbt_assets, dbt_project
from national_tourism.resources.minio_resource import minio_resource
from national_tourism.resources.duckdb_resource import duckdb_resource
from national_tourism.resources.dbt_resource import dbt_resource
from national_tourism.resources.http_resource import http_resource
from national_tourism.schedules.daily_schedule import daily_tourism_schedule
from national_tourism.sensors.new_file_sensor import new_raw_file_sensor
from national_tourism.sensors.alert_sensor import pipeline_failure_alert

# ---------------------------------------------------------------------------
# Cargar assets de ingesta (Bronze layer — Python)
# ---------------------------------------------------------------------------
ingestion_assets = load_assets_from_package_module(
    ingestion,
    group_name="ingestion",  # Bronze layer
)

# ---------------------------------------------------------------------------
# Definición del proyecto
# ---------------------------------------------------------------------------
defs = Definitions(
    assets=[*ingestion_assets, national_tourism_dbt_assets],
    resources={
        "minio": minio_resource,
        "duckdb": duckdb_resource,
        "dbt": dbt_resource,
        "http": http_resource,
    },
    schedules=[daily_tourism_schedule],
    sensors=[new_raw_file_sensor, pipeline_failure_alert],
)
