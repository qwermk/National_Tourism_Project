# =============================================================================
# dbt Assets — Orquestación de modelos dbt desde Dagster
# =============================================================================
# Este módulo expone todos los modelos dbt como assets de Dagster.
#
# Flujo unificado (sin duplicación):
#   raw_tourism_arrivals ──┐
#                          ├─► @dbt_assets ──► staging views ──► gold tables
#   raw_hotel_occupancy  ──┘
#
# El CustomDagsterDbtTranslator mapea los sources Bronze de dbt a los
# asset keys de los assets de ingesta, creando la cadena de dependencias.
# =============================================================================

from pathlib import Path
from typing import Any, Mapping

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

# ---------------------------------------------------------------------------
# Proyecto dbt — usa DbtProject para gestionar manifesto automáticamente
# dagster/national_tourism/assets/ → project_root/dbt/
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

# Genera target/manifest.json en entornos de desarrollo.
# En producción (Docker) el manifesto debe generarse en el Dockerfile
# con: RUN dbt parse --profiles-dir /app/dbt --project-dir /app/dbt
dbt_project.prepare_if_dev()


# ---------------------------------------------------------------------------
# Translator personalizado: mapea sources Bronze de dbt → assets de ingesta
# ---------------------------------------------------------------------------
class NationalTourismDbtTranslator(DagsterDbtTranslator):
    """
    Mapea los dbt sources del bucket Bronze a los Dagster asset keys
    producidos por los assets de ingesta de Python, estableciendo la
    cadena de dependencias: ingestion → dbt staging → dbt marts.
    """

    # Mapeo de dbt sources → Dagster asset keys de ingesta.
    # Cada source definido en _sources.yml debe apuntar al asset Python
    # que produce el Parquet correspondiente en MinIO.
    _SOURCE_TO_ASSET: dict[tuple[str, str], str] = {
        ("bronze", "tourism_arrivals"): "raw_tourism_arrivals",
        ("bronze", "hotel_occupancy"): "raw_hotel_occupancy",
        ("bronze", "citur_arrivals"): "raw_citur_arrivals",
        ("bronze", "citur_hotel_occupancy"): "raw_citur_hotel_occupancy",
        ("world_bank", "arrivals_annual"): "raw_world_bank_arrivals",
        ("world_bank", "regional_comparison"): "raw_world_bank_regional",
        ("dane", "tourism_gdp"): "raw_dane_tourism_gdp",
        ("migracion", "flows"): "raw_migracion_flows",
        ("aerocivil", "passengers"): "raw_aerocivil_passengers",
        ("banrep", "tourism_balance"): "raw_banrep_tourism_balance",
    }

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props.get("resource_type", "")
        if resource_type == "source":
            source_name = dbt_resource_props.get("source_name", "")
            name = dbt_resource_props.get("name", "")
            asset_name = self._SOURCE_TO_ASSET.get((source_name, name))
            if asset_name:
                return AssetKey(asset_name)
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Asigna grupos basados en la config dbt (+tags)."""
        tags = dbt_resource_props.get("config", {}).get("tags", [])
        if "gold" in tags or "marts" in tags:
            return "marts"
        if "staging" in tags or "silver" in tags:
            return "staging"
        return "dbt"


# ---------------------------------------------------------------------------
# Asset principal: ejecuta dbt build (seed + run + test)
# ---------------------------------------------------------------------------
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=NationalTourismDbtTranslator(),
    name="national_tourism_dbt",
)
def national_tourism_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    """
    Ejecuta el pipeline dbt completo:
    1. dbt seed  — carga seed_departments.csv en DuckDB
    2. dbt run   — materializa modelos staging → marts
    3. dbt test  — valida calidad de datos

    Equivalente a `dbt build` que ejecuta los tres pasos en orden.

    Outputs en DuckDB:
    - staging.stg_tourism_arrivals  (view)
    - staging.stg_hotel_occupancy   (view)
    - gold.fct_tourism_arrivals     (table)
    - gold.fct_hotel_occupancy      (table)
    - gold.dim_departments          (table, 33 departamentos)
    """
    yield from dbt.cli(["build"], context=context).stream()
