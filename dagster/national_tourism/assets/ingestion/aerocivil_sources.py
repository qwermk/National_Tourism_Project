# =============================================================================
# Bronze Layer — Fuente: Aerocivil (Pasajeros aéreos de Colombia)
# =============================================================================
#
# Descarga estadísticas de pasajeros aéreos nacionales e internacionales
# por aeropuerto, desde datos abiertos de la Aeronáutica Civil de Colombia.
#
# Variable de entorno:
#   AEROCIVIL_PASSENGERS_URL → URL del dataset CSV en datos.gov.co
#                              (opcional: si no se configura, genera sintéticos)
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
    name="raw_aerocivil_passengers",
    description=(
        "Estadísticas de pasajeros aéreos por aeropuerto en Colombia — Aerocivil. "
        "Incluye pasajeros nacionales e internacionales, llegadas y salidas."
    ),
    metadata={
        "layer": "bronze",
        "source": "Aerocivil / datos.gov.co",
        "format": "parquet",
        "update_frequency": "monthly",
    },
    compute_kind="python",
)
def raw_aerocivil_passengers(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """Descarga datos de pasajeros aéreos de la Aerocivil."""
    aerocivil_url = os.getenv("AEROCIVIL_PASSENGERS_URL")
    source_used = "synthetic"
    df = None

    if aerocivil_url:
        context.log.info(f"Descargando pasajeros aéreos desde: {aerocivil_url}")
        try:
            raw_csv = http.get_text(aerocivil_url)
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"aerocivil/passengers_{date.today().isoformat()}.csv",
                data=raw_csv.encode("utf-8"),
                content_type="text/csv",
            )
            df = pd.read_csv(io.StringIO(raw_csv), encoding="utf-8")
            df = _normalize_aerocivil(df)
            source_used = "aerocivil_datos_gov_co"
            context.log.info(f"Aerocivil: {len(df)} filas descargadas")
        except Exception as exc:
            context.log.error(f"Error descargando Aerocivil: {exc}. Usando sintéticos.")

    if df is None or df.empty:
        context.log.warning("Usando datos sintéticos de Aerocivil.")
        df = _generate_synthetic_passengers()
        source_used = "synthetic"

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="aerocivil/passengers.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "fuente_utilizada": MetadataValue.text(source_used),
            "anio_min": MetadataValue.int(int(df["year"].min())),
            "anio_max": MetadataValue.int(int(df["year"].max())),
            "aeropuertos_unicos": MetadataValue.int(df["airport"].nunique()),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/aerocivil/passengers.parquet"),
        }
    )


_AEROCIVIL_COL_MAP = {
    "año": "year", "anio": "year", "year": "year",
    "mes": "month", "month": "month",
    "aeropuerto": "airport", "airport": "airport",
    "ciudad": "airport_city", "city": "airport_city",
    "pasajeros_nacionales": "domestic_passengers",
    "pasajeros_internacionales": "international_passengers",
    "total_pasajeros": "total_passengers",
    "tipo_vuelo": "flight_type",
}


def _normalize_aerocivil(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names from Aerocivil CSV variants."""
    rename = {}
    for col in df.columns:
        key = col.strip().lower().replace("-", "_").replace(" ", "_")
        if key in _AEROCIVIL_COL_MAP:
            rename[col] = _AEROCIVIL_COL_MAP[key]
    df = df.rename(columns=rename)

    for col_name, default in [
        ("year", None), ("month", None),
        ("airport", "Desconocido"), ("airport_city", "Desconocido"),
        ("domestic_passengers", 0), ("international_passengers", 0),
        ("total_passengers", 0),
    ]:
        if col_name not in df.columns and default is not None:
            df[col_name] = default

    df["year"] = pd.to_numeric(df.get("year"), errors="coerce").fillna(0).astype(int)
    df["month"] = pd.to_numeric(df.get("month"), errors="coerce").fillna(0).astype(int)
    df["domestic_passengers"] = pd.to_numeric(df.get("domestic_passengers"), errors="coerce").fillna(0).astype(int)
    df["international_passengers"] = pd.to_numeric(df.get("international_passengers"), errors="coerce").fillna(0).astype(int)

    if "total_passengers" not in df.columns or df["total_passengers"].sum() == 0:
        df["total_passengers"] = df["domestic_passengers"] + df["international_passengers"]
    else:
        df["total_passengers"] = pd.to_numeric(df["total_passengers"], errors="coerce").fillna(0).astype(int)

    df = df[(df["year"] >= 2010) & df["month"].between(1, 12)]
    return df


def _generate_synthetic_passengers() -> pd.DataFrame:
    """Genera datos sintéticos de pasajeros aéreos."""
    rng = np.random.default_rng(seed=888)

    airports = {
        "El Dorado": "Bogotá",
        "José María Córdova": "Medellín",
        "Rafael Núñez": "Cartagena",
        "Alfonso Bonilla Aragón": "Cali",
        "Ernesto Cortissoz": "Barranquilla",
        "Simón Bolívar": "Santa Marta",
        "Matecaña": "Pereira",
        "Camilo Daza": "Cúcuta",
        "Gustavo Rojas Pinilla": "San Andrés",
        "Palonegro": "Bucaramanga",
    }

    # Proporción del tráfico total por aeropuerto
    traffic_share = {
        "El Dorado": 0.45, "José María Córdova": 0.15,
        "Rafael Núñez": 0.08, "Alfonso Bonilla Aragón": 0.08,
        "Ernesto Cortissoz": 0.05, "Simón Bolívar": 0.04,
        "Matecaña": 0.04, "Camilo Daza": 0.04,
        "Gustavo Rojas Pinilla": 0.04, "Palonegro": 0.03,
    }

    # Proporción de internacional por aeropuerto
    intl_share = {
        "El Dorado": 0.35, "José María Córdova": 0.20,
        "Rafael Núñez": 0.25, "Alfonso Bonilla Aragón": 0.10,
        "Ernesto Cortissoz": 0.08, "Simón Bolívar": 0.05,
        "Matecaña": 0.05, "Camilo Daza": 0.03,
        "Gustavo Rojas Pinilla": 0.15, "Palonegro": 0.03,
    }

    records = []
    for year in range(2010, 2027):
        base_monthly = 3_500_000  # Pasajeros mensuales base Colombia
        if year >= 2015:
            base_monthly *= 1 + (year - 2015) * 0.06
        if year == 2020:
            base_monthly *= 0.30
        elif year == 2021:
            base_monthly *= 0.55
        elif year >= 2022:
            base_monthly *= 1 + (year - 2021) * 0.08

        for month in range(1, 13):
            seasonal = 1.0
            if month in (1, 6, 7, 12):
                seasonal = 1.25
            elif month in (2, 9):
                seasonal = 0.85

            if year == 2020 and month >= 3:
                seasonal *= 0.15

            monthly_total = base_monthly * seasonal

            for airport_name, city in airports.items():
                share = traffic_share[airport_name]
                intl = intl_share[airport_name]
                total = int(monthly_total * share * rng.uniform(0.85, 1.15))
                intl_pax = int(total * intl * rng.uniform(0.8, 1.2))
                dom_pax = total - intl_pax

                records.append({
                    "year": year,
                    "month": month,
                    "airport": airport_name,
                    "airport_city": city,
                    "domestic_passengers": max(0, dom_pax),
                    "international_passengers": max(0, intl_pax),
                    "total_passengers": max(0, total),
                })

    return pd.DataFrame(records)
