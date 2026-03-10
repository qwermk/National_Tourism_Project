# =============================================================================
# Bronze Layer — Ingesta de datos de llegadas de turistas
# =============================================================================
# Este asset descarga datos crudos de turismo desde fuentes públicas
# y los deposita en MinIO (bucket: bronze) en formato Parquet.
#
# Fuentes:
#   - CITUR (Centro de Información Turística de Colombia)
#   - Archivos CSV locales en data/raw/
# =============================================================================

import os
from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)


@asset(
    description="Datos crudos de llegadas de turistas internacionales a Colombia (Bronze layer).",
    metadata={
        "layer": "bronze",
        "source": "CITUR / CSV local",
        "format": "parquet",
    },
    compute_kind="python",
)
def raw_tourism_arrivals(
    context: AssetExecutionContext,
    minio,
) -> MaterializeResult:
    """
    Ingesta datos de llegadas de turistas internacionales.

    Flujo:
    1. Lee archivos CSV desde data/raw/ (o descarga de fuente externa)
    2. Convierte a Parquet
    3. Sube a MinIO bucket 'bronze'
    """
    raw_dir = Path("data/raw")

    # Intentar leer desde archivos locales
    csv_files = list(raw_dir.glob("*arrivals*.csv")) + list(raw_dir.glob("*llegadas*.csv"))

    if csv_files:
        context.log.info(f"Encontrados {len(csv_files)} archivos CSV de llegadas.")
        dfs = [pd.read_csv(f, encoding="latin-1") for f in csv_files]
        df = pd.concat(dfs, ignore_index=True)
    else:
        # Crear un dataset de ejemplo para desarrollo
        context.log.warning("No se encontraron CSVs. Creando dataset de ejemplo.")
        df = _create_sample_arrivals_data()

    # Subir a MinIO como Parquet
    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="tourism_arrivals/arrivals.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "num_columns": MetadataValue.int(len(df.columns)),
            "columns": MetadataValue.text(", ".join(df.columns.tolist())),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/tourism_arrivals/arrivals.parquet"),
        }
    )


@asset(
    description="Datos crudos de ocupación hotelera en Colombia (Bronze layer).",
    metadata={
        "layer": "bronze",
        "source": "CITUR / CSV local",
        "format": "parquet",
    },
    compute_kind="python",
)
def raw_hotel_occupancy(
    context: AssetExecutionContext,
    minio,
) -> MaterializeResult:
    """
    Ingesta datos de ocupación hotelera por departamento.
    """
    raw_dir = Path("data/raw")
    csv_files = list(raw_dir.glob("*ocupacion*.csv")) + list(raw_dir.glob("*occupancy*.csv"))

    if csv_files:
        context.log.info(f"Encontrados {len(csv_files)} archivos CSV de ocupación.")
        dfs = [pd.read_csv(f, encoding="latin-1") for f in csv_files]
        df = pd.concat(dfs, ignore_index=True)
    else:
        context.log.warning("No se encontraron CSVs. Creando dataset de ejemplo.")
        df = _create_sample_occupancy_data()

    minio.upload_dataframe_as_parquet(
        bucket_name="bronze",
        object_name="hotel_occupancy/occupancy.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "num_columns": MetadataValue.int(len(df.columns)),
            "columns": MetadataValue.text(", ".join(df.columns.tolist())),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("bronze/hotel_occupancy/occupancy.parquet"),
        }
    )


# =============================================================================
# Funciones auxiliares — Datos de ejemplo para desarrollo
# =============================================================================


def _create_sample_arrivals_data() -> pd.DataFrame:
    """Genera datos de ejemplo de llegadas de turistas para desarrollo."""
    import numpy as np

    np.random.seed(42)

    countries = [
        "Estados Unidos", "Brasil", "México", "Argentina", "Ecuador",
        "Perú", "Chile", "España", "Alemania", "Francia",
        "Reino Unido", "Canadá", "Panamá", "Venezuela", "Costa Rica",
    ]
    departments = [
        "Bogotá D.C.", "Antioquia", "Bolívar", "Valle del Cauca",
        "Atlántico", "Santander", "San Andrés", "Magdalena",
        "Nariño", "Risaralda",
    ]

    n_records = 5000
    dates = pd.date_range("2019-01-01", "2024-12-31", periods=n_records)

    return pd.DataFrame({
        "fecha_llegada": dates,
        "anio": dates.year,
        "mes": dates.month,
        "pais_origen": np.random.choice(countries, n_records),
        "departamento_destino": np.random.choice(departments, n_records),
        "motivo_viaje": np.random.choice(
            ["Turismo", "Negocios", "Eventos", "Educación", "Salud"],
            n_records,
            p=[0.55, 0.20, 0.10, 0.08, 0.07],
        ),
        "punto_entrada": np.random.choice(
            ["Aéreo", "Terrestre", "Marítimo"],
            n_records,
            p=[0.70, 0.22, 0.08],
        ),
        "numero_visitantes": np.random.randint(1, 500, n_records),
        "gasto_estimado_usd": np.round(np.random.uniform(200, 5000, n_records), 2),
    })


def _create_sample_occupancy_data() -> pd.DataFrame:
    """Genera datos de ejemplo de ocupación hotelera."""
    import numpy as np

    np.random.seed(123)

    departments = [
        "Bogotá D.C.", "Antioquia", "Bolívar", "Valle del Cauca",
        "Atlántico", "Santander", "San Andrés", "Magdalena",
        "Nariño", "Risaralda", "Quindío", "Boyacá",
    ]

    records = []
    for year in range(2019, 2025):
        for month in range(1, 13):
            for dept in departments:
                # Simular estacionalidad (temporada alta en dic, ene, jun, jul)
                base_occupancy = 45.0
                if month in [1, 6, 7, 12]:
                    base_occupancy += 20.0
                elif month in [3, 4, 10]:
                    base_occupancy += 10.0

                # Efecto COVID en 2020
                if year == 2020 and month >= 3:
                    base_occupancy *= 0.3

                occupancy = min(95, max(10, base_occupancy + np.random.normal(0, 8)))

                records.append({
                    "anio": year,
                    "mes": month,
                    "departamento": dept,
                    "porcentaje_ocupacion": round(occupancy, 1),
                    "habitaciones_disponibles": np.random.randint(500, 5000),
                    "habitaciones_ocupadas": None,  # Se calculará en staging
                    "tarifa_promedio_cop": round(np.random.uniform(80000, 500000), 0),
                })

    df = pd.DataFrame(records)
    # Calcular habitaciones ocupadas
    df["habitaciones_ocupadas"] = (
        df["habitaciones_disponibles"] * df["porcentaje_ocupacion"] / 100
    ).astype(int)

    return df
