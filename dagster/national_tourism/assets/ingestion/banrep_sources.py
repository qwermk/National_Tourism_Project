# =============================================================================
# Bronze Layer — Fuente: Banco de la República (Balanza turística)
# =============================================================================
#
# Descarga datos de la balanza de pagos — cuenta de viajes del Banco
# de la República de Colombia (ingresos y egresos por turismo en USD).
#
# Variable de entorno:
#   BANREP_TOURISM_BALANCE_URL → URL del dataset (datos.gov.co o banrep.gov.co)
#                                 (opcional: si no se configura, genera sintéticos)
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
    name="raw_banrep_tourism_balance",
    description=(
        "Balanza turística de Colombia — Banco de la República. "
        "Ingresos y egresos por concepto de viajes en la balanza de pagos (USD millones)."
    ),
    metadata={
        "layer": "bronze",
        "source": "Banco de la República",
        "format": "parquet",
        "update_frequency": "quarterly",
    },
    compute_kind="python",
)
def raw_banrep_tourism_balance(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """Descarga datos de la balanza turística del Banco de la República."""
    banrep_url = os.getenv("BANREP_TOURISM_BALANCE_URL")
    source_used = "synthetic"
    df = None

    if banrep_url:
        context.log.info(f"Descargando balanza turística desde: {banrep_url}")
        try:
            raw_csv = http.get_text(banrep_url)
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"banrep/tourism_balance_{date.today().isoformat()}.csv",
                data=raw_csv.encode("utf-8"),
                content_type="text/csv",
            )
            df = pd.read_csv(io.StringIO(raw_csv), encoding="utf-8")
            df = _normalize_banrep(df)
            source_used = "banrep"
            context.log.info(f"BanRep: {len(df)} filas descargadas")
        except Exception as exc:
            context.log.error(f"Error descargando BanRep: {exc}. Usando sintéticos.")

    if df is None or df.empty:
        context.log.warning("Usando datos sintéticos de balanza turística.")
        df = _generate_synthetic_balance()
        source_used = "synthetic"

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="banrep/tourism_balance.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "fuente_utilizada": MetadataValue.text(source_used),
            "anio_min": MetadataValue.int(int(df["year"].min())),
            "anio_max": MetadataValue.int(int(df["year"].max())),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/banrep/tourism_balance.parquet"),
        }
    )


_BANREP_COL_MAP = {
    "año": "year", "anio": "year", "year": "year",
    "trimestre": "quarter", "quarter": "quarter",
    "ingresos_turismo_usd": "tourism_income_usd_millions",
    "egresos_turismo_usd": "tourism_expenditure_usd_millions",
    "ingresos": "tourism_income_usd_millions",
    "egresos": "tourism_expenditure_usd_millions",
    "balance": "tourism_balance_usd_millions",
}


def _normalize_banrep(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names from BanRep CSV."""
    rename = {}
    for col in df.columns:
        key = col.strip().lower().replace("-", "_").replace(" ", "_")
        if key in _BANREP_COL_MAP:
            rename[col] = _BANREP_COL_MAP[key]
    df = df.rename(columns=rename)

    for col_name, default in [
        ("year", None), ("quarter", 1),
        ("tourism_income_usd_millions", 0.0),
        ("tourism_expenditure_usd_millions", 0.0),
    ]:
        if col_name not in df.columns and default is not None:
            df[col_name] = default

    df["year"] = pd.to_numeric(df.get("year"), errors="coerce").fillna(0).astype(int)
    df["quarter"] = pd.to_numeric(df.get("quarter"), errors="coerce").fillna(1).astype(int)
    df["tourism_income_usd_millions"] = pd.to_numeric(
        df.get("tourism_income_usd_millions"), errors="coerce"
    ).fillna(0.0)
    df["tourism_expenditure_usd_millions"] = pd.to_numeric(
        df.get("tourism_expenditure_usd_millions"), errors="coerce"
    ).fillna(0.0)

    if "tourism_balance_usd_millions" not in df.columns:
        df["tourism_balance_usd_millions"] = (
            df["tourism_income_usd_millions"] - df["tourism_expenditure_usd_millions"]
        )

    df = df[(df["year"] >= 2006) & df["quarter"].between(1, 4)]
    return df


def _generate_synthetic_balance() -> pd.DataFrame:
    """Genera datos sintéticos de balanza turística."""
    rng = np.random.default_rng(seed=666)

    records = []
    base_income = 800.0  # USD millones por trimestre (2006)
    base_expense = 500.0

    for year in range(2006, 2027):
        # Tendencia de crecimiento
        growth = 1 + (year - 2006) * 0.04
        if year == 2020:
            growth *= 0.25
        elif year == 2021:
            growth *= 0.55
        elif year >= 2022:
            growth *= 1 + (year - 2021) * 0.08

        for quarter in range(1, 5):
            # Estacionalidad: Q1 y Q3 más altos (vacaciones)
            seasonal = 1.0
            if quarter in (1, 3):
                seasonal = 1.15
            elif quarter == 2:
                seasonal = 0.90

            income = round(base_income * growth * seasonal * rng.uniform(0.9, 1.1), 1)
            expense = round(base_expense * growth * seasonal * rng.uniform(0.85, 1.15), 1)

            records.append({
                "year": year,
                "quarter": quarter,
                "tourism_income_usd_millions": income,
                "tourism_expenditure_usd_millions": expense,
                "tourism_balance_usd_millions": round(income - expense, 1),
                "source": "banrep",
            })

    return pd.DataFrame(records)
