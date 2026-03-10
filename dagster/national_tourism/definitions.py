# =============================================================================
# Dagster Definitions — Punto de entrada principal
# =============================================================================
# Este archivo registra todos los assets, resources, schedules y sensors
# del proyecto de turismo nacional.
# =============================================================================

from dagster import Definitions, load_assets_from_package_module

from national_tourism.assets import ingestion, staging, marts
from national_tourism.resources.minio_resource import minio_resource
from national_tourism.resources.duckdb_resource import duckdb_resource
from national_tourism.schedules.daily_schedule import daily_tourism_schedule
from national_tourism.sensors.new_file_sensor import new_raw_file_sensor

# ---------------------------------------------------------------------------
# Cargar assets por módulo (cada módulo = una capa del Medallion)
# ---------------------------------------------------------------------------
ingestion_assets = load_assets_from_package_module(
    ingestion,
    group_name="ingestion",  # Bronze layer
)

staging_assets = load_assets_from_package_module(
    staging,
    group_name="staging",  # Silver layer
)

marts_assets = load_assets_from_package_module(
    marts,
    group_name="marts",  # Gold layer
)

# ---------------------------------------------------------------------------
# Definición del proyecto
# ---------------------------------------------------------------------------
defs = Definitions(
    assets=[*ingestion_assets, *staging_assets, *marts_assets],
    resources={
        "minio": minio_resource,
        "duckdb": duckdb_resource,
    },
    schedules=[daily_tourism_schedule],
    sensors=[new_raw_file_sensor],
)
