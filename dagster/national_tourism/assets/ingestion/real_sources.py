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
    "ST.INT.ARVL": "llegadas_internacionales",          # Arrivals
    "ST.INT.DPRT": "salidas_residentes",                 # Departures
    "ST.INT.RCPT.CD": "ingresos_turismo_usd_corrientes", # Receipts (current USD)
}

# Año mínimo para filtrar datos históricos obsoletos
_MIN_YEAR = 2010

# ---------------------------------------------------------------------------
# Helpers: normalización de columnas CITUR
# ---------------------------------------------------------------------------

# Mapeo flexible de nombres de columna que suele usar CITUR en sus exports
_ARRIVALS_COL_MAP = {
    # Año
    "año": "anio", "anio": "anio", "year": "anio", "vigencia": "anio",
    # Mes
    "mes": "mes", "month": "mes", "periodo": "mes",
    # País
    "pais": "pais_origen", "país": "pais_origen",
    "pais_origen": "pais_origen", "country": "pais_origen",
    "pais de residencia": "pais_origen", "país de residencia": "pais_origen",
    # Departamento
    "departamento": "departamento_destino",
    "departamento_destino": "departamento_destino",
    "destino": "departamento_destino",
    # Visitantes
    "visitantes": "numero_visitantes", "llegadas": "numero_visitantes",
    "total": "numero_visitantes", "arrivals": "numero_visitantes",
    "viajeros": "numero_visitantes", "turistas": "numero_visitantes",
    "numero_viajeros": "numero_visitantes",
    # Gasto
    "gasto": "gasto_estimado_usd", "gasto_usd": "gasto_estimado_usd",
    "spending": "gasto_estimado_usd",
    # Motivo
    "motivo": "motivo_viaje", "motivo_viaje": "motivo_viaje",
    "tipo_viaje": "motivo_viaje",
    # Punto entrada
    "punto_entrada": "punto_entrada", "entrada": "punto_entrada",
    "tipo_ingreso": "punto_entrada", "via": "punto_entrada",
}

_OCCUPANCY_COL_MAP = {
    "año": "anio", "anio": "anio", "year": "anio", "vigencia": "anio",
    "mes": "mes", "month": "mes",
    "departamento": "departamento", "region": "departamento",
    "ocupacion": "porcentaje_ocupacion",
    "porcentaje_ocupacion": "porcentaje_ocupacion",
    "tasa_ocupacion": "porcentaje_ocupacion",
    "ocupacion_hotelera": "porcentaje_ocupacion",
    "habitaciones_disponibles": "habitaciones_disponibles",
    "cuartos_disponibles": "habitaciones_disponibles",
    "habitaciones_ocupadas": "habitaciones_ocupadas",
    "habitaciones_vendidas": "habitaciones_ocupadas",
    "tarifa_promedio": "tarifa_promedio_cop",
    "tarifa_promedio_cop": "tarifa_promedio_cop",
    "adr": "tarifa_promedio_cop",
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
        "fecha_llegada": None,
        "anio": None,
        "mes": None,
        "pais_origen": "Desconocido",
        "departamento_destino": "Desconocido",
        "motivo_viaje": "Turismo",
        "punto_entrada": "Aéreo",
        "numero_visitantes": 0,
        "gasto_estimado_usd": 0.0,
    }
    for col, default in required.items():
        if col not in df.columns:
            if default is not None:
                context.log.warning(f"Columna '{col}' no encontrada; usando default '{default}'.")
                df[col] = default
            # fecha_llegada, anio, mes se derivan a continuación

    # Derivar anio/mes desde fecha_llegada si vienen en esa columna
    if "fecha_llegada" in df.columns and ("anio" not in df.columns or "mes" not in df.columns):
        df["fecha_llegada"] = pd.to_datetime(df["fecha_llegada"], errors="coerce")
        if "anio" not in df.columns:
            df["anio"] = df["fecha_llegada"].dt.year
        if "mes" not in df.columns:
            df["mes"] = df["fecha_llegada"].dt.month

    # Si no hay fecha_llegada, construirla desde anio/mes
    if "fecha_llegada" not in df.columns or df["fecha_llegada"].isna().all():
        df["fecha_llegada"] = pd.to_datetime(
            {"year": df["anio"], "month": df["mes"], "day": 15}, errors="coerce"
        )

    df["numero_visitantes"] = pd.to_numeric(df["numero_visitantes"], errors="coerce").fillna(0).astype(int)
    df["gasto_estimado_usd"] = pd.to_numeric(df["gasto_estimado_usd"], errors="coerce").fillna(0.0)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").fillna(0).astype(int)
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)

    # Filtrar filas sin año/mes válidos
    df = df[(df["anio"] >= _MIN_YEAR) & df["mes"].between(1, 12)]
    return df


def _complete_occupancy_df(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
    """Asegura que el DataFrame de ocupación tenga todas las columnas requeridas."""
    required = {
        "anio": None,
        "mes": None,
        "departamento": "Desconocido",
        "porcentaje_ocupacion": 50.0,
        "habitaciones_disponibles": 0,
        "habitaciones_ocupadas": None,  # calculado
        "tarifa_promedio_cop": 0.0,
    }
    for col, default in required.items():
        if col not in df.columns and default is not None:
            context.log.warning(f"Columna '{col}' no encontrada; usando default.")
            df[col] = default

    df["porcentaje_ocupacion"] = (
        pd.to_numeric(df["porcentaje_ocupacion"], errors="coerce").fillna(50.0).clip(0, 100)
    )
    df["habitaciones_disponibles"] = (
        pd.to_numeric(df["habitaciones_disponibles"], errors="coerce").fillna(0).astype(int)
    )
    if "habitaciones_ocupadas" not in df.columns or df["habitaciones_ocupadas"].isna().all():
        df["habitaciones_ocupadas"] = (
            df["habitaciones_disponibles"] * df["porcentaje_ocupacion"] / 100
        ).astype(int)
    else:
        df["habitaciones_ocupadas"] = (
            pd.to_numeric(df["habitaciones_ocupadas"], errors="coerce").fillna(0).astype(int)
        )
    df["tarifa_promedio_cop"] = (
        pd.to_numeric(df["tarifa_promedio_cop"], errors="coerce").fillna(0.0)
    )
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").fillna(0).astype(int)
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)
    df = df[(df["anio"] >= _MIN_YEAR) & df["mes"].between(1, 12)]
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
                    "anio": int(item["date"]),
                    "indicador_codigo": indicator_code,
                    "indicador_nombre": indicator_name,
                    "valor": float(item["value"]),
                    "pais_codigo": "COL",
                    "pais_nombre": "Colombia",
                    "fuente": "world_bank",
                })
            context.log.info(
                f"World Bank {indicator_code}: descargados "
                f"{sum(1 for r in records if r['indicador_codigo'] == indicator_code)} registros."
            )
        except Exception as exc:
            errors.append(f"{indicator_code}: {exc}")
            context.log.error(f"Error descargando {indicator_code}: {exc}")

    if not records:
        context.log.warning("No se pudieron descargar datos del World Bank — usando datos mínimos de referencia.")
        records = [
            {"anio": y, "indicador_codigo": "ST.INT.ARVL",
             "indicador_nombre": "llegadas_internacionales",
             "valor": 0.0, "pais_codigo": "COL",
             "pais_nombre": "Colombia", "fuente": "fallback"}
            for y in range(_MIN_YEAR, date.today().year + 1)
        ]

    df = pd.DataFrame(records)
    df = df[df["anio"] >= _MIN_YEAR].sort_values(["indicador_codigo", "anio"])

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="world_bank/arrivals_annual.parquet",
        df=df,
    )

    arrivals_count = df[df["indicador_codigo"] == "ST.INT.ARVL"]
    latest_arrivals = (
        arrivals_count.sort_values("anio").tail(1)[["anio", "valor"]].to_dict("records")
        if not arrivals_count.empty else []
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "indicadores": MetadataValue.text(", ".join(_WB_INDICATORS.keys())),
            "anio_min": MetadataValue.int(int(df["anio"].min())),
            "anio_max": MetadataValue.int(int(df["anio"].max())),
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
            "anio_min": MetadataValue.int(int(df["anio"].min()) if not df.empty else 0),
            "anio_max": MetadataValue.int(int(df["anio"].max()) if not df.empty else 0),
            "paises_unicos": MetadataValue.int(df["pais_origen"].nunique()),
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
            "departamentos_unicos": MetadataValue.int(df["departamento"].nunique()),
            "anio_min": MetadataValue.int(int(df["anio"].min()) if not df.empty else 0),
            "anio_max": MetadataValue.int(int(df["anio"].max()) if not df.empty else 0),
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
    for year in range(_MIN_YEAR, 2025):
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
                    "fecha_llegada": f"{year}-{month:02d}-15",
                    "anio": year,
                    "mes": month,
                    "pais_origen": country,
                    "departamento_destino": dept,
                    "motivo_viaje": rng.choice(
                        ["Turismo", "Negocios", "Eventos", "Educación", "Salud"],
                        p=[0.55, 0.20, 0.10, 0.08, 0.07],
                    ),
                    "punto_entrada": rng.choice(
                        ["Aéreo", "Terrestre", "Marítimo"], p=[0.70, 0.22, 0.08]
                    ),
                    "numero_visitantes": visitors,
                    "gasto_estimado_usd": round(visitors * rng.uniform(800, 2500), 2),
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
    for year in range(_MIN_YEAR, 2025):
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
                    "anio": year,
                    "mes": month,
                    "departamento": dept,
                    "porcentaje_ocupacion": round(occupancy, 1),
                    "habitaciones_disponibles": hab_disp,
                    "habitaciones_ocupadas": int(hab_disp * occupancy / 100),
                    "tarifa_promedio_cop": round(rng.uniform(80_000, 500_000), 0),
                })
    return pd.DataFrame(records)
