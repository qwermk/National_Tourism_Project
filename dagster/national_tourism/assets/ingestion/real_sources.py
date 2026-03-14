# =============================================================================
# Bronze Layer — Conectores a Fuentes Reales de Turismo
# =============================================================================
#
# Assets de ingesta que consumen datos públicos oficiales de turismo colombiano:
#
#   raw_world_bank_arrivals   — World Bank Open Data API (ST.INT.ARVL)
#                               Totales anuales de llegadas a Colombia.
#                               Siempre disponible (API pública sin auth).
#
#   raw_citur_arrivals        — CITUR / datos.gov.co
#                               Llegadas mensuales por país y departamento.
#                               Requiere: CITUR_ARRIVALS_URL
#
#   raw_citur_hotel_occupancy — CITUR / datos.gov.co
#                               Ocupación hotelera mensual por departamento.
#                               Requiere: CITUR_OCCUPANCY_URL
#
# Todas las assets escriben al bucket «bronze» en MinIO y hacen fallback
# a datos sintéticos si la fuente externa no está disponible.
#
# Variables de entorno:
#   CITUR_ARRIVALS_URL    URL de export CSV del dataset de llegadas en
#                         datos.gov.co (p.ej. de tipo rows.csv)
#   CITUR_OCCUPANCY_URL   URL de export CSV del dataset de ocupación
#   WORLD_BANK_BASE_URL   (opcional) override de la URL base del World Bank
# =============================================================================

import io
import json
import os
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from national_tourism.resources.http_resource import HttpResource
from national_tourism.resources.minio_resource import MinIOResource

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_WORLD_BANK_BASE = os.getenv(
    "WORLD_BANK_BASE_URL", "https://api.worldbank.org/v2"
)

# Indicadores World Bank relevantes para turismo colombiano
_WB_INDICATORS = {
    "ST.INT.ARVL": "international_arrivals",          # Arrivals
    "ST.INT.DPRT": "resident_departures",             # Departures
    "ST.INT.RCPT.CD": "tourism_receipts_current_usd", # Receipts (current USD)
    "ST.INT.XPND.CD": "tourism_expenditure_usd",      # Expenditure abroad (USD)
    "ST.INT.RCPT.XP.ZS": "tourism_pct_of_exports",    # Tourism as % of exports
    "NY.GDP.MKTP.CD": "gdp_current_usd",              # Total GDP (USD)
}

# Países vecinos / región para comparación
_WB_REGIONAL_COUNTRIES = {
    "COL": "Colombia",
    "ECU": "Ecuador",
    "PER": "Perú",
    "PAN": "Panamá",
    "BRA": "Brasil",
    "MEX": "México",
    "CRI": "Costa Rica",
}

# Indicadores para comparación regional (los más relevantes)
_WB_REGIONAL_INDICATORS = {
    "ST.INT.ARVL": "international_arrivals",
    "ST.INT.RCPT.CD": "tourism_receipts_current_usd",
}

# Año mínimo para filtrar datos históricos obsoletos
_MIN_YEAR = 2010

# Año máximo para datos sintéticos (actualizar cada año)
_MAX_YEAR = 2026

# ---------------------------------------------------------------------------
# Helpers: normalización de columnas CITUR
# ---------------------------------------------------------------------------

# Mapeo flexible de nombres de columna que suele usar CITUR en sus exports
_ARRIVALS_COL_MAP = {
    # Year
    "año": "year", "anio": "year", "year": "year", "vigencia": "year",
    # Month
    "mes": "month", "month": "month", "periodo": "month",
    # Country
    "pais": "country_of_origin", "país": "country_of_origin",
    "pais_origen": "country_of_origin", "country": "country_of_origin",
    "pais de residencia": "country_of_origin", "país de residencia": "country_of_origin",
    # Department
    "departamento": "destination_department",
    "departamento_destino": "destination_department",
    "destino": "destination_department",
    # Visitors
    "visitantes": "number_of_visitors", "llegadas": "number_of_visitors",
    "total": "number_of_visitors", "arrivals": "number_of_visitors",
    "viajeros": "number_of_visitors", "turistas": "number_of_visitors",
    "numero_viajeros": "number_of_visitors",
    # Spending
    "gasto": "estimated_spending_usd", "gasto_usd": "estimated_spending_usd",
    "spending": "estimated_spending_usd",
    # Purpose
    "motivo": "travel_purpose", "motivo_viaje": "travel_purpose",
    "tipo_viaje": "travel_purpose",
    # Entry point
    "punto_entrada": "entry_point", "entrada": "entry_point",
    "tipo_ingreso": "entry_point", "via": "entry_point",
}

_OCCUPANCY_COL_MAP = {
    "año": "year", "anio": "year", "year": "year", "vigencia": "year",
    "mes": "month", "month": "month",
    "departamento": "department", "region": "department",
    "ocupacion": "occupancy_rate",
    "porcentaje_ocupacion": "occupancy_rate",
    "tasa_ocupacion": "occupancy_rate",
    "ocupacion_hotelera": "occupancy_rate",
    "habitaciones_disponibles": "available_rooms",
    "cuartos_disponibles": "available_rooms",
    "habitaciones_ocupadas": "occupied_rooms",
    "habitaciones_vendidas": "occupied_rooms",
    "tarifa_promedio": "average_rate_cop",
    "tarifa_promedio_cop": "average_rate_cop",
    "adr": "average_rate_cop",
}


def _normalize_columns(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """Renombra columnas usando el mapa tolerante a variantes de nombres."""
    rename = {}
    for col in df.columns:
        key = col.strip().lower().replace("-", "_").replace(" ", "_")
        if key in col_map:
            rename[col] = col_map[key]
    return df.rename(columns=rename)


# ---------------------------------------------------------------------------
# Helpers: completar columnas faltantes con valores por defecto
# ---------------------------------------------------------------------------

def _complete_arrivals_df(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
    """Asegura que el DataFrame de llegadas tenga todas las columnas requeridas."""
    required = {
        "arrival_date": None,
        "year": None,
        "month": None,
        "country_of_origin": "Desconocido",
        "destination_department": "Desconocido",
        "travel_purpose": "Turismo",
        "entry_point": "Aéreo",
        "number_of_visitors": 0,
        "estimated_spending_usd": 0.0,
    }
    for col, default in required.items():
        if col not in df.columns:
            if default is not None:
                context.log.warning(f"Columna '{col}' no encontrada; usando default '{default}'.")
                df[col] = default
            # arrival_date, year, month se derivan a continuación

    # Derivar year/month desde arrival_date si vienen en esa columna
    if "arrival_date" in df.columns and ("year" not in df.columns or "month" not in df.columns):
        df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce")
        if "year" not in df.columns:
            df["year"] = df["arrival_date"].dt.year
        if "month" not in df.columns:
            df["month"] = df["arrival_date"].dt.month

    # Si no hay arrival_date, construirla desde year/month
    if "arrival_date" not in df.columns or df["arrival_date"].isna().all():
        df["arrival_date"] = pd.to_datetime(
            {"year": df["year"], "month": df["month"], "day": 15}, errors="coerce"
        )

    df["number_of_visitors"] = pd.to_numeric(df["number_of_visitors"], errors="coerce").fillna(0).astype(int)
    df["estimated_spending_usd"] = pd.to_numeric(df["estimated_spending_usd"], errors="coerce").fillna(0.0)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)

    # Filtrar filas sin año/mes válidos
    df = df[(df["year"] >= _MIN_YEAR) & df["month"].between(1, 12)]
    return df


def _complete_occupancy_df(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
    """Asegura que el DataFrame de ocupación tenga todas las columnas requeridas."""
    required = {
        "year": None,
        "month": None,
        "department": "Desconocido",
        "occupancy_rate": 50.0,
        "available_rooms": 0,
        "occupied_rooms": None,  # calculado
        "average_rate_cop": 0.0,
    }
    for col, default in required.items():
        if col not in df.columns and default is not None:
            context.log.warning(f"Columna '{col}' no encontrada; usando default.")
            df[col] = default

    df["occupancy_rate"] = (
        pd.to_numeric(df["occupancy_rate"], errors="coerce").fillna(50.0).clip(0, 100)
    )
    df["available_rooms"] = (
        pd.to_numeric(df["available_rooms"], errors="coerce").fillna(0).astype(int)
    )
    if "occupied_rooms" not in df.columns or df["occupied_rooms"].isna().all():
        df["occupied_rooms"] = (
            df["available_rooms"] * df["occupancy_rate"] / 100
        ).astype(int)
    else:
        df["occupied_rooms"] = (
            pd.to_numeric(df["occupied_rooms"], errors="coerce").fillna(0).astype(int)
        )
    df["average_rate_cop"] = (
        pd.to_numeric(df["average_rate_cop"], errors="coerce").fillna(0.0)
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0).astype(int)
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
    df = df[(df["year"] >= _MIN_YEAR) & df["month"].between(1, 12)]
    return df


# ---------------------------------------------------------------------------
# Asset 1: World Bank — Totales anuales de turismo en Colombia
# ---------------------------------------------------------------------------

@asset(
    name="raw_world_bank_arrivals",
    description=(
        "Datos anuales del World Bank Open Data API: llegadas internacionales "
        "a Colombia (ST.INT.ARVL), salidas de residentes (ST.INT.DPRT) e "
        "ingresos por turismo (ST.INT.RCPT.CD). Fuente siempre disponible, sin autenticación."
    ),
    metadata={
        "layer": "bronze",
        "source": "World Bank Open Data API",
        "url": "https://api.worldbank.org/v2/country/COL/indicator/ST.INT.ARVL",
        "format": "parquet",
        "update_frequency": "annual",
    },
    compute_kind="python",
)
def raw_world_bank_arrivals(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """
    Descarga indicadores de turismo de Colombia desde la API del World Bank.

    Indicadores descargados:
      ST.INT.ARVL  — Llegadas de turistas internacionales
      ST.INT.DPRT  — Salidas de residentes al exterior
      ST.INT.RCPT.CD — Ingresos por turismo en USD corrientes
    """
    records = []
    errors = []

    for indicator_code, indicator_name in _WB_INDICATORS.items():
        url = f"{_WORLD_BANK_BASE}/country/COL/indicator/{indicator_code}"
        params = {"format": "json", "per_page": "100", "date": f"{_MIN_YEAR}:2024"}

        try:
            data = http.get_json(url, params=params)
            # World Bank API returns: [metadata, [records...]]
            if not isinstance(data, list) or len(data) < 2 or not data[1]:
                context.log.warning(f"No data returned for indicator {indicator_code}.")
                continue

            # Guardar respuesta JSON original en raw/
            raw_json = json.dumps(data, ensure_ascii=False).encode("utf-8")
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"world_bank/{indicator_code}_{date.today().isoformat()}.json",
                data=raw_json,
                content_type="application/json",
            )
            for item in data[1]:
                if item.get("value") is None:
                    continue
                records.append({
                    "year": int(item["date"]),
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_name,
                    "value": float(item["value"]),
                    "country_code": "COL",
                    "country_name": "Colombia",
                    "source": "world_bank",
                })
            context.log.info(
                f"World Bank {indicator_code}: descargados "
                f"{sum(1 for r in records if r['indicator_code'] == indicator_code)} registros."
            )
        except Exception as exc:
            errors.append(f"{indicator_code}: {exc}")
            context.log.error(f"Error descargando {indicator_code}: {exc}")

    if not records:
        context.log.warning("No se pudieron descargar datos del World Bank — usando datos mínimos de referencia.")
        records = [
            {"year": y, "indicator_code": "ST.INT.ARVL",
             "indicator_name": "international_arrivals",
             "value": 0.0, "country_code": "COL",
             "country_name": "Colombia", "source": "fallback"}
            for y in range(_MIN_YEAR, date.today().year + 1)
        ]

    df = pd.DataFrame(records)
    df = df[df["year"] >= _MIN_YEAR].sort_values(["indicator_code", "year"])

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="world_bank/arrivals_annual.parquet",
        df=df,
    )

    arrivals_count = df[df["indicator_code"] == "ST.INT.ARVL"]
    latest_arrivals = (
        arrivals_count.sort_values("year").tail(1)[["year", "value"]].to_dict("records")
        if not arrivals_count.empty else []
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "indicadores": MetadataValue.text(", ".join(_WB_INDICATORS.keys())),
            "anio_min": MetadataValue.int(int(df["year"].min())),
            "anio_max": MetadataValue.int(int(df["year"].max())),
            "ultimo_registro_llegadas": MetadataValue.text(str(latest_arrivals)),
            "errores": MetadataValue.text(", ".join(errors) if errors else "ninguno"),
            "minio_path": MetadataValue.text("bronze/world_bank/arrivals_annual.parquet"),
        }
    )


# ---------------------------------------------------------------------------
# Asset 2: CITUR — Llegadas mensuales (datos.gov.co)
# ---------------------------------------------------------------------------

@asset(
    name="raw_citur_arrivals",
    description=(
        "Llegadas de turistas internacionales a Colombia desde CITUR / datos.gov.co. "
        "Requiere la variable de entorno CITUR_ARRIVALS_URL con la URL del export CSV "
        "del dataset en datos.gov.co. Si no está configurada o falla, usa datos sintéticos."
    ),
    metadata={
        "layer": "bronze",
        "source": "CITUR / datos.gov.co",
        "config_env_var": "CITUR_ARRIVALS_URL",
        "format": "parquet",
        "update_frequency": "monthly",
        "datos_gov_co_info": (
            "Dataset: 'Viajeros no residentes — llegadas'. "
            "Buscar en datos.gov.co → Turismo → CITUR. "
            "URL de descarga CSV: https://www.datos.gov.co/api/views/{ID}/rows.csv"
        ),
    },
    compute_kind="python",
)
def raw_citur_arrivals(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """
    Descarga datos de llegadas de CITUR vía datos.gov.co.

    Configurar CITUR_ARRIVALS_URL con la URL del dataset en datos.gov.co.
    Formato esperado: CSV con columnas que incluyan país, mes, año y número de visitors.
    """
    citur_url = os.getenv("CITUR_ARRIVALS_URL")
    source_used = "synthetic"
    df: Optional[pd.DataFrame] = None

    if citur_url:
        context.log.info(f"Descargando llegadas CITUR desde: {citur_url}")
        try:
            # Descargar texto CSV crudo y guardar cópia en raw/ antes de parsear
            raw_csv_text = http.get_text(citur_url)
            raw_csv_bytes = raw_csv_text.encode("utf-8", errors="replace")
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"citur/tourism_arrivals_{date.today().isoformat()}.csv",
                data=raw_csv_bytes,
                content_type="text/csv",
            )
            context.log.info("CSV original de CITUR arrivals guardado en raw/citur/")

            raw_df = pd.read_csv(io.StringIO(raw_csv_text), encoding="utf-8")
            context.log.info(
                f"CITUR arrivals descargado: {len(raw_df)} filas, columnas: {list(raw_df.columns)}"
            )
            raw_df = _normalize_columns(raw_df, _ARRIVALS_COL_MAP)
            df = _complete_arrivals_df(raw_df, context)
            source_used = "citur_datos_gov_co"
            context.log.info(f"CITUR arrivals procesado: {len(df)} filas limpias.")
        except Exception as exc:
            context.log.error(
                f"Error descargando datos CITUR de '{citur_url}': {exc}. "
                f"Usando datos sintéticos como fallback."
            )
    else:
        context.log.warning(
            "CITUR_ARRIVALS_URL no configurado. "
            "Para usar datos reales, descarga el CSV de 'Viajeros no residentes' "
            "desde datos.gov.co y asigna la URL a CITUR_ARRIVALS_URL."
            " Usando datos sintéticos."
        )

    if df is None or df.empty:
        df = _generate_synthetic_arrivals()
        source_used = "synthetic"

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="citur/tourism_arrivals.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "num_columns": MetadataValue.int(len(df.columns)),
            "fuente_utilizada": MetadataValue.text(source_used),
            "anio_min": MetadataValue.int(int(df["year"].min()) if not df.empty else 0),
            "anio_max": MetadataValue.int(int(df["year"].max()) if not df.empty else 0),
            "paises_unicos": MetadataValue.int(df["country_of_origin"].nunique()),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/citur/tourism_arrivals.parquet"),
        }
    )


# ---------------------------------------------------------------------------
# Asset 3: CITUR — Ocupación hotelera mensual (datos.gov.co)
# ---------------------------------------------------------------------------

@asset(
    name="raw_citur_hotel_occupancy",
    description=(
        "Ocupación hotelera mensual por departamento desde CITUR / datos.gov.co. "
        "Requiere CITUR_OCCUPANCY_URL con la URL del export CSV del dataset. "
        "Fallback a datos sintéticos si no está configurado."
    ),
    metadata={
        "layer": "bronze",
        "source": "CITUR / datos.gov.co",
        "config_env_var": "CITUR_OCCUPANCY_URL",
        "format": "parquet",
        "update_frequency": "monthly",
        "datos_gov_co_info": (
            "Dataset: 'Encuesta de ocupación hotelera'. "
            "Buscar en datos.gov.co → Turismo → CITUR / DANE."
        ),
    },
    compute_kind="python",
)
def raw_citur_hotel_occupancy(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """
    Descarga datos de ocupación hotelera desde CITUR vía datos.gov.co.

    Configurar CITUR_OCCUPANCY_URL con la URL del dataset en datos.gov.co.
    """
    citur_url = os.getenv("CITUR_OCCUPANCY_URL")
    source_used = "synthetic"
    df: Optional[pd.DataFrame] = None

    if citur_url:
        context.log.info(f"Descargando ocupación hotelera CITUR desde: {citur_url}")
        try:
            # Descargar texto CSV crudo y guardar cópia en raw/ antes de parsear
            raw_csv_text = http.get_text(citur_url)
            raw_csv_bytes = raw_csv_text.encode("utf-8", errors="replace")
            minio.upload_bytes(
                bucket_name="raw",
                object_name=f"citur/hotel_occupancy_{date.today().isoformat()}.csv",
                data=raw_csv_bytes,
                content_type="text/csv",
            )
            context.log.info("CSV original de CITUR occupancy guardado en raw/citur/")

            raw_df = pd.read_csv(io.StringIO(raw_csv_text), encoding="utf-8")
            context.log.info(
                f"CITUR occupancy descargado: {len(raw_df)} filas, columnas: {list(raw_df.columns)}"
            )
            raw_df = _normalize_columns(raw_df, _OCCUPANCY_COL_MAP)
            df = _complete_occupancy_df(raw_df, context)
            source_used = "citur_datos_gov_co"
            context.log.info(f"CITUR occupancy procesado: {len(df)} filas limpias.")
        except Exception as exc:
            context.log.error(
                f"Error descargando datos CITUR de '{citur_url}': {exc}. "
                f"Usando datos sintéticos como fallback."
            )
    else:
        context.log.warning(
            "CITUR_OCCUPANCY_URL no configurado. "
            "Para usar datos reales descarga el CSV de la encuesta EOH de DANE/CITUR "
            "en datos.gov.co y asigna la URL a CITUR_OCCUPANCY_URL. "
            "Usando datos sintéticos."
        )

    if df is None or df.empty:
        df = _generate_synthetic_occupancy()
        source_used = "synthetic"

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="citur/hotel_occupancy.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "num_columns": MetadataValue.int(len(df.columns)),
            "fuente_utilizada": MetadataValue.text(source_used),
            "departamentos_unicos": MetadataValue.int(df["department"].nunique()),
            "anio_min": MetadataValue.int(int(df["year"].min()) if not df.empty else 0),
            "anio_max": MetadataValue.int(int(df["year"].max()) if not df.empty else 0),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/citur/hotel_occupancy.parquet"),
        }
    )


# ---------------------------------------------------------------------------
# Generadores sintéticos (fallback — mismos esquemas)
# ---------------------------------------------------------------------------

def _generate_synthetic_arrivals() -> pd.DataFrame:
    """Genera datos de llegadas sintéticos con esquema compatible con dbt."""
    rng = np.random.default_rng(42)
    countries = {
        "Estados Unidos": 0.22, "Brasil": 0.08, "México": 0.07,
        "Argentina": 0.06, "Ecuador": 0.09, "Perú": 0.05,
        "Chile": 0.05, "España": 0.04, "Alemania": 0.03,
        "Francia": 0.03, "Reino Unido": 0.03, "Canadá": 0.04,
        "Panamá": 0.05, "Venezuela": 0.08, "Costa Rica": 0.03,
        "Italia": 0.02, "Países Bajos": 0.02, "Otro": 0.11,
    }
    departments = [
        "Bogotá D.C.", "Antioquia", "Bolívar", "Valle Del Cauca",
        "Atlántico", "Santander", "San Andrés", "Magdalena",
        "Nariño", "Risaralda", "Quindío", "Boyacá",
    ]
    records = []
    for year in range(_MIN_YEAR, _MAX_YEAR + 1):
        for month in range(1, 13):
            base = 35_000
            if month in (1, 7, 12):
                base = int(base * 1.4)
            elif month in (6, 8):
                base = int(base * 1.2)
            elif month in (2, 9):
                base = int(base * 0.85)
            if year == 2020 and month >= 3:
                base = int(base * 0.15)
            elif year == 2021 and month <= 6:
                base = int(base * 0.5)
            elif year >= 2022:
                base = int(base * (1 + (year - 2021) * 0.12))
            for country, share in countries.items():
                dept = departments[rng.integers(0, len(departments))]
                visitors = max(0, int(base * share * rng.uniform(0.03, 0.18)))
                if visitors == 0:
                    continue
                records.append({
                    "arrival_date": f"{year}-{month:02d}-15",
                    "year": year,
                    "month": month,
                    "country_of_origin": country,
                    "destination_department": dept,
                    "travel_purpose": rng.choice(
                        ["Turismo", "Negocios", "Eventos", "Educación", "Salud"],
                        p=[0.55, 0.20, 0.10, 0.08, 0.07],
                    ),
                    "entry_point": rng.choice(
                        ["Aéreo", "Terrestre", "Marítimo"], p=[0.70, 0.22, 0.08]
                    ),
                    "number_of_visitors": visitors,
                    "estimated_spending_usd": round(visitors * rng.uniform(800, 2500), 2),
                })
    return pd.DataFrame(records)


def _generate_synthetic_occupancy() -> pd.DataFrame:
    """Genera datos de ocupación hotelera sintéticos con esquema compatible con dbt."""
    rng = np.random.default_rng(123)
    departments = [
        "Bogotá D.C.", "Antioquia", "Bolívar", "Valle Del Cauca",
        "Atlántico", "Santander", "San Andrés", "Magdalena",
        "Nariño", "Risaralda", "Quindío", "Boyacá",
    ]
    records = []
    for year in range(_MIN_YEAR, _MAX_YEAR + 1):
        for month in range(1, 13):
            for dept in departments:
                base_occ = 48.0
                if month in (1, 6, 7, 12):
                    base_occ += 18.0
                elif month in (3, 4, 10):
                    base_occ += 8.0
                if year == 2020 and month >= 3:
                    base_occ *= 0.25
                elif year == 2021 and month <= 6:
                    base_occ *= 0.6
                if dept in ("San Andrés", "Bolívar", "Magdalena"):
                    base_occ = min(base_occ + 12, 95)
                occupancy = float(np.clip(base_occ + rng.normal(0, 7), 5, 95))
                hab_disp = int(rng.integers(500, 4000))
                records.append({
                    "year": year,
                    "month": month,
                    "department": dept,
                    "occupancy_rate": round(occupancy, 1),
                    "available_rooms": hab_disp,
                    "occupied_rooms": int(hab_disp * occupancy / 100),
                    "average_rate_cop": round(rng.uniform(80_000, 500_000), 0),
                })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Asset 4: World Bank — Comparación regional con países vecinos
# ---------------------------------------------------------------------------

@asset(
    name="raw_world_bank_regional",
    description=(
        "Indicadores de turismo de países vecinos (ECU, PER, PAN, BRA, MEX, CRI) "
        "desde la API del World Bank, para comparación regional con Colombia."
    ),
    metadata={
        "layer": "bronze",
        "source": "World Bank Open Data API",
        "format": "parquet",
        "update_frequency": "annual",
    },
    compute_kind="python",
)
def raw_world_bank_regional(
    context: AssetExecutionContext,
    minio: MinIOResource,
    http: HttpResource,
) -> MaterializeResult:
    """Descarga indicadores de turismo para Colombia y países vecinos."""
    records = []
    errors = []

    for country_code, country_name in _WB_REGIONAL_COUNTRIES.items():
        for indicator_code, indicator_name in _WB_REGIONAL_INDICATORS.items():
            url = f"{_WORLD_BANK_BASE}/country/{country_code}/indicator/{indicator_code}"
            params = {"format": "json", "per_page": "100", "date": f"{_MIN_YEAR}:2024"}
            try:
                data = http.get_json(url, params=params)
                if not isinstance(data, list) or len(data) < 2 or not data[1]:
                    continue
                for item in data[1]:
                    if item.get("value") is None:
                        continue
                    records.append({
                        "year": int(item["date"]),
                        "country_code": country_code,
                        "country_name": country_name,
                        "indicator_code": indicator_code,
                        "indicator_name": indicator_name,
                        "value": float(item["value"]),
                        "source": "world_bank",
                    })
            except Exception as exc:
                errors.append(f"{country_code}/{indicator_code}: {exc}")
                context.log.warning(f"Error {country_code} {indicator_code}: {exc}")

    if not records:
        context.log.warning("No regional data — generating synthetic fallback.")
        records = _generate_synthetic_regional()

    df = pd.DataFrame(records)
    df = df[df["year"] >= _MIN_YEAR].sort_values(["country_code", "indicator_code", "year"])

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="world_bank/regional_comparison.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "countries": MetadataValue.text(", ".join(df["country_code"].unique())),
            "indicators": MetadataValue.text(", ".join(df["indicator_code"].unique())),
            "errores": MetadataValue.text(", ".join(errors) if errors else "ninguno"),
            "minio_path": MetadataValue.text("bronze/world_bank/regional_comparison.parquet"),
        }
    )


def _generate_synthetic_regional() -> list[dict]:
    """Fallback: datos sintéticos de comparación regional."""
    rng = np.random.default_rng(seed=999)
    base_arrivals = {
        "COL": 4_500_000, "MEX": 45_000_000, "BRA": 6_500_000,
        "PER": 4_400_000, "ECU": 2_400_000, "PAN": 2_500_000, "CRI": 3_100_000,
    }
    base_receipts = {
        "COL": 6_600, "MEX": 26_000, "BRA": 5_900,
        "PER": 4_800, "ECU": 2_300, "PAN": 6_800, "CRI": 4_000,
    }
    records = []
    for country_code, country_name in _WB_REGIONAL_COUNTRIES.items():
        for year in range(_MIN_YEAR, 2025):
            factor = 1 + (year - _MIN_YEAR) * 0.03
            if year == 2020:
                factor *= 0.25
            elif year == 2021:
                factor *= 0.55
            records.append({
                "year": year,
                "country_code": country_code,
                "country_name": country_name,
                "indicator_code": "ST.INT.ARVL",
                "indicator_name": "international_arrivals",
                "value": round(base_arrivals.get(country_code, 1_000_000) * factor * rng.uniform(0.9, 1.1)),
                "source": "synthetic",
            })
            records.append({
                "year": year,
                "country_code": country_code,
                "country_name": country_name,
                "indicator_code": "ST.INT.RCPT.CD",
                "indicator_name": "tourism_receipts_current_usd",
                "value": round(base_receipts.get(country_code, 3_000) * 1_000_000 * factor * rng.uniform(0.9, 1.1)),
                "source": "synthetic",
            })
    return records
