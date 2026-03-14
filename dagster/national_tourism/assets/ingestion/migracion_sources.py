# =============================================================================
# Bronze Layer — Fuente: Migración Colombia (Flujos migratorios)
# =============================================================================
#
# ¿Qué hace este archivo?
#   Descarga datos de Migración Colombia sobre entradas y salidas de
#   viajeros por nacionalidad y punto de control migratorio.
#
# ¿De dónde vienen los datos?
#   - API de datos abiertos de Migración Colombia (datos.gov.co)
#   - URL de descarga directa CSV configurada en variable de entorno
#
# ¿A dónde van los datos?
#   Se guardan como Parquet en MinIO, bucket "bronze"
#   Ruta: bronze/migracion/flows.parquet
#
# Variable de entorno necesaria:
#   MIGRACION_FLOWS_URL  →  URL del dataset CSV en datos.gov.co
#                            (opcional: si no se configura, usa datos sintéticos)
# =============================================================================

import io
import os
from datetime import date

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from national_tourism.resources.http_resource import HttpResource
from national_tourism.resources.minio_resource import MinIOResource


@asset(
    name="raw_migracion_flows",
    description=(
        "Flujos migratorios de entrada/salida de viajeros a Colombia, "
        "desde Migración Colombia. Incluye: nacionalidad, tipo de movimiento "
        "(entrada/salida), punto de control y cantidad de viajeros."
    ),
    metadata={
        "layer": "bronze",
        "source": "Migración Colombia / datos.gov.co",
        "format": "parquet",
        "update_frequency": "monthly",
    },
    compute_kind="python",
)
def raw_migracion_flows(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """
    Descarga datos de flujos migratorios de Migración Colombia.

    Pasos:
    1. Intenta descargar CSV real desde datos.gov.co
    2. Si falla, genera datos sintéticos de respaldo
    3. Guarda como Parquet en MinIO (bucket bronze)
    """
    migracion_url = os.getenv("MIGRACION_FLOWS_URL")
    source_used = "synthetic"
    df = None

    # --- Paso 1: Intentar descarga real ---
    if migracion_url:
        context.log.info(f"Descargando flujos migratorios desde: {migracion_url}")
        try:
            raw_csv = http.get_text(migracion_url)
            # Guardar copia original en raw
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"migracion/flows_{date.today().isoformat()}.csv",
                data=raw_csv.encode("utf-8"),
                content_type="text/csv",
            )
            df = pd.read_csv(io.StringIO(raw_csv), encoding="utf-8")
            source_used = "migracion_datos_gov_co"
            context.log.info(f"Migración Colombia: {len(df)} filas descargadas")
        except Exception as exc:
            context.log.error(f"Error descargando Migración: {exc}. Usando datos sintéticos.")

    # --- Paso 2: Datos sintéticos si no hay datos reales ---
    if df is None or df.empty:
        context.log.warning(
            "MIGRACION_FLOWS_URL no configurado. Usando datos sintéticos."
        )
        df = _generate_synthetic_migration_flows()
        source_used = "synthetic"

    # --- Paso 3: Guardar en MinIO ---
    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="migracion/flows.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "fuente_utilizada": MetadataValue.text(source_used),
            "anio_min": MetadataValue.int(int(df["year"].min())),
            "anio_max": MetadataValue.int(int(df["year"].max())),
            "nacionalidades_unicas": MetadataValue.int(df["nationality"].nunique()),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/migracion/flows.parquet"),
        }
    )


def _generate_synthetic_migration_flows() -> pd.DataFrame:
    """
    Genera datos sintéticos de flujos migratorios.

    Columnas:
    - year, month: Periodo del registro
    - nationality: País de origen del viajero
    - movement_type: 'entrada' o 'salida'
    - control_point: Ciudad del punto de control migratorio
    - number_of_travelers: Número de viajeros registrados
    """
    rng = np.random.default_rng(seed=555)

    # Principales nacionalidades que visitan Colombia
    nacionalidades = {
        "Estados Unidos": 0.20, "Venezuela": 0.15, "Ecuador": 0.10,
        "México": 0.08, "Brasil": 0.07, "Argentina": 0.06,
        "Perú": 0.06, "Chile": 0.05, "España": 0.04,
        "Panamá": 0.04, "Canadá": 0.03, "Alemania": 0.03,
        "Francia": 0.03, "Reino Unido": 0.02, "Italia": 0.02,
        "Otros": 0.02,
    }

    # Principales puntos de control migratorio
    puntos_control = [
        "Bogotá - El Dorado",
        "Cartagena - Rafael Núñez",
        "Medellín - José María Córdova",
        "Cali - Alfonso Bonilla Aragón",
        "Cúcuta - Frontera terrestre",
        "Ipiales - Frontera terrestre",
        "Santa Marta - Simón Bolívar",
        "Barranquilla - Ernesto Cortissoz",
    ]

    records = []
    for year in range(2015, 2027):
        for month in range(1, 13):
            # Base mensual
            base_entradas = 80_000
            base_salidas = 60_000

            # Estacionalidad: más viajeros en vacaciones
            if month in (1, 6, 7, 12):
                base_entradas = int(base_entradas * 1.35)
                base_salidas = int(base_salidas * 1.30)

            # COVID-19
            if year == 2020 and month >= 3:
                base_entradas = int(base_entradas * 0.10)
                base_salidas = int(base_salidas * 0.10)
            elif year == 2021 and month <= 6:
                base_entradas = int(base_entradas * 0.45)
                base_salidas = int(base_salidas * 0.40)

            # Crecimiento post-COVID
            if year >= 2022:
                factor = 1 + (year - 2021) * 0.10
                base_entradas = int(base_entradas * factor)
                base_salidas = int(base_salidas * factor)

            for nacionalidad, proporcion in nacionalidades.items():
                punto = rng.choice(puntos_control)

                # Entradas
                entradas = max(0, int(base_entradas * proporcion * rng.uniform(0.7, 1.3)))
                if entradas > 0:
                    records.append({
                        "year": year,
                        "month": month,
                        "nationality": nacionalidad,
                        "movement_type": "entrada",
                        "control_point": punto,
                        "number_of_travelers": entradas,
                        "source": "migracion_colombia",
                    })

                # Salidas (generalmente menos que entradas)
                salidas = max(0, int(base_salidas * proporcion * rng.uniform(0.6, 1.2)))
                if salidas > 0:
                    records.append({
                        "year": year,
                        "month": month,
                        "nationality": nacionalidad,
                        "movement_type": "salida",
                        "control_point": punto,
                        "number_of_travelers": salidas,
                        "source": "migracion_colombia",
                    })

    return pd.DataFrame(records)
