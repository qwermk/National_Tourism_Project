# =============================================================================
# Bronze Layer — Fuente: DANE (PIB Turístico de Colombia)
# =============================================================================
#
# ¿Qué hace este archivo?
#   Descarga datos del DANE (Departamento Administrativo Nacional de
#   Estadística) sobre la contribución del turismo al PIB de Colombia.
#
# ¿De dónde vienen los datos?
#   - API de datos abiertos del DANE (datos.gov.co)
#   - Si la API no está disponible, genera datos sintéticos de respaldo
#
# ¿A dónde van los datos?
#   Se guardan como archivo Parquet en MinIO, bucket "bronze"
#   Ruta: bronze/dane/tourism_gdp.parquet
#
# Variable de entorno necesaria:
#   DANE_TOURISM_GDP_URL  →  URL del dataset CSV en datos.gov.co
#                             (opcional: si no se configura, usa datos sintéticos)
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
    name="raw_dane_tourism_gdp",
    description=(
        "Contribución del turismo al PIB de Colombia, desde el DANE. "
        "Incluye: PIB turístico en miles de millones COP, porcentaje del PIB total, "
        "y empleo generado por turismo. Datos anuales desde 2010."
    ),
    metadata={
        "layer": "bronze",
        "source": "DANE / datos.gov.co",
        "format": "parquet",
        "update_frequency": "annual",
    },
    compute_kind="python",
)
def raw_dane_tourism_gdp(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """
    Descarga datos de PIB turístico del DANE.

    Pasos:
    1. Intenta descargar CSV real desde datos.gov.co
    2. Si falla o no está configurado, genera datos sintéticos
    3. Guarda el resultado como Parquet en MinIO (bucket bronze)
    """
    dane_url = os.getenv("DANE_TOURISM_GDP_URL")
    source_used = "synthetic"
    df = None

    # --- Paso 1: Intentar descarga real ---
    if dane_url:
        context.log.info(f"Descargando PIB turístico DANE desde: {dane_url}")
        try:
            raw_csv = http.get_text(dane_url)
            # Guardar copia original en bucket raw
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"dane/tourism_gdp_{date.today().isoformat()}.csv",
                data=raw_csv.encode("utf-8"),
                content_type="text/csv",
            )
            df = pd.read_csv(io.StringIO(raw_csv), encoding="utf-8")
            source_used = "dane_datos_gov_co"
            context.log.info(f"DANE PIB turístico: {len(df)} filas descargadas")
        except Exception as exc:
            context.log.error(f"Error descargando DANE: {exc}. Usando datos sintéticos.")

    # --- Paso 2: Datos sintéticos si no hay datos reales ---
    if df is None or df.empty:
        context.log.warning(
            "DANE_TOURISM_GDP_URL no configurado o falló la descarga. "
            "Usando datos sintéticos de referencia."
        )
        df = _generate_synthetic_gdp()
        source_used = "synthetic"

    # --- Paso 3: Guardar en MinIO como Parquet ---
    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="dane/tourism_gdp.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "fuente_utilizada": MetadataValue.text(source_used),
            "anio_min": MetadataValue.int(int(df["year"].min())),
            "anio_max": MetadataValue.int(int(df["year"].max())),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/dane/tourism_gdp.parquet"),
        }
    )


def _generate_synthetic_gdp() -> pd.DataFrame:
    """
    Genera datos sintéticos de PIB turístico para desarrollo.

    Columnas generadas:
    - year: Año (2010-2026)
    - tourism_gdp_billions_cop: PIB turístico en billones de pesos colombianos
    - pct_of_total_gdp: Porcentaje del PIB total que representa el turismo
    - tourism_employment_thousands: Empleos generados por turismo (en miles)
    - annual_variation_pct: Cambio porcentual respecto al año anterior
    """
    rng = np.random.default_rng(seed=777)

    records = []
    pib_anterior = 25.0  # Base: 25 billones COP en 2010

    for year in range(2010, 2027):
        # Crecimiento base: ~4% anual
        crecimiento = 0.04

        # Efecto COVID-19
        if year == 2020:
            crecimiento = -0.55  # Caída fuerte
        elif year == 2021:
            crecimiento = 0.35  # Recuperación parcial
        elif year == 2022:
            crecimiento = 0.25  # Recuperación fuerte
        elif year >= 2023:
            crecimiento = 0.08 + rng.uniform(-0.02, 0.02)

        pib_turismo = pib_anterior * (1 + crecimiento)
        variacion = ((pib_turismo - pib_anterior) / pib_anterior) * 100

        records.append({
            "year": year,
            "tourism_gdp_billions_cop": round(pib_turismo, 2),
            "pct_of_total_gdp": round(rng.uniform(1.8, 3.5), 2),
            "tourism_employment_thousands": int(rng.uniform(1200, 1900)),
            "annual_variation_pct": round(variacion, 2),
            "source": "dane",
        })
        pib_anterior = pib_turismo

    return pd.DataFrame(records)
