# =============================================================================
# Dagster Definitions — Punto de entrada principal del pipeline
# =============================================================================
#
# ¿Qué es este archivo?
#   Es el archivo principal que Dagster lee al iniciar. Aquí se registran
#   TODOS los componentes del pipeline: assets, resources, schedules y sensors.
#
# ¿Cómo funciona el pipeline?
#   El pipeline tiene 2 etapas:
#
#   ETAPA 1 — Ingesta (Python):
#     Los assets de Python descargan datos crudos de fuentes externas
#     (World Bank, CITUR, DANE, Migración Colombia) y los guardan
#     como archivos Parquet en MinIO (bucket "bronze").
#
#   ETAPA 2 — Transformación (dbt):
#     El @dbt_assets ejecuta los modelos SQL de dbt que transforman
#     los datos Bronze → Silver (limpieza) → Gold (modelos de negocio)
#     directamente en DuckDB.
#
#   Flujo completo:
#     Fuentes externas → [Python] → MinIO (Bronze)
#                                      ↓
#     [dbt staging SQL]  → DuckDB (Silver: vistas)
#                                      ↓
#     [dbt marts SQL]    → DuckDB (Gold: tablas)
#                                      ↓
#     Streamlit Dashboard ← consultas SQL ← DuckDB
#
# =============================================================================

from dagster import Definitions, load_assets_from_package_module

# --- Assets de ingesta (Python → MinIO Bronze) ---
from national_tourism.assets import ingestion

# --- Assets de dbt (SQL → DuckDB Silver/Gold) ---
from national_tourism.assets.dbt_assets import national_tourism_dbt_assets, dbt_project

# --- Resources: conexiones a servicios externos ---
from national_tourism.resources.minio_resource import minio_resource
from national_tourism.resources.duckdb_resource import duckdb_resource
from national_tourism.resources.dbt_resource import dbt_resource
from national_tourism.resources.http_resource import http_resource

# --- Schedules y Sensors ---
from national_tourism.schedules.daily_schedule import daily_tourism_schedule
from national_tourism.sensors.new_file_sensor import new_raw_file_sensor
from national_tourism.sensors.alert_sensor import pipeline_failure_alert

# ---------------------------------------------------------------------------
# Cargar assets de ingesta (Bronze layer — Python)
# Esto importa TODOS los assets del paquete ingestion/ automáticamente:
#   raw_tourism_arrivals, raw_hotel_occupancy,
#   raw_world_bank_arrivals, raw_citur_arrivals, raw_citur_hotel_occupancy,
#   raw_dane_tourism_gdp, raw_migracion_flows
# ---------------------------------------------------------------------------
ingestion_assets = load_assets_from_package_module(
    ingestion,
    group_name="ingestion",  # Se muestran agrupados en la UI de Dagster
)

# ---------------------------------------------------------------------------
# Definición del proyecto — Dagster lee esto al iniciar
# ---------------------------------------------------------------------------
defs = Definitions(
    # Assets: ingesta (Python) + transformación (dbt)
    assets=[*ingestion_assets, national_tourism_dbt_assets],

    # Resources: conexiones necesarias para los assets
    resources={
        "minio": minio_resource,    # Almacenamiento S3-compatible
        "duckdb": duckdb_resource,  # Motor OLAP local
        "dbt": dbt_resource,        # CLI de dbt
        "http": http_resource,      # Cliente HTTP para APIs
    },

    # Schedule: ejecución automática diaria
    schedules=[daily_tourism_schedule],

    # Sensors: detectan archivos nuevos + alertas de fallos
    sensors=[new_raw_file_sensor, pipeline_failure_alert],
)
