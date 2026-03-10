# =============================================================================
# Gold Layer — Modelos de negocio (Marts)
# =============================================================================
# Este módulo genera los modelos finales listos para consumo:
#   - Fact tables: métricas de turismo agregadas
#   - Dimension tables: departamentos, países, tiempo
#
# Estos modelos alimentan los dashboards de Evidence.dev
# =============================================================================

import pandas as pd
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)


@asset(
    deps=["stg_tourism_arrivals"],
    description="Fact table: llegadas de turistas agregadas por mes, país y departamento (Gold layer).",
    metadata={
        "layer": "gold",
        "format": "parquet",
        "type": "fact_table",
    },
    compute_kind="duckdb",
)
def fct_tourism_arrivals(
    context: AssetExecutionContext,
    minio,
    duckdb,
) -> MaterializeResult:
    """
    Genera la fact table de llegadas de turistas agregada.

    Métricas:
    - Total visitantes por mes/país/departamento
    - Gasto total y promedio
    """
    # Leer datos Silver
    df = minio.read_parquet_as_dataframe(
        bucket_name="silver",
        object_name="tourism_arrivals/arrivals_clean.parquet",
    )

    # Agregar por mes, país de origen y departamento
    if "fecha_llegada" in df.columns:
        df["anio_mes"] = df["fecha_llegada"].dt.to_period("M").astype(str)
    elif "anio" in df.columns and "mes" in df.columns:
        df["anio_mes"] = df["anio"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2)

    agg_df = (
        df.groupby(["anio_mes", "pais_origen", "departamento_destino", "motivo_viaje"])
        .agg(
            total_visitantes=("numero_visitantes", "sum"),
            gasto_total_usd=("gasto_estimado_usd", "sum"),
            gasto_promedio_usd=("gasto_estimado_usd", "mean"),
            num_registros=("numero_visitantes", "count"),
        )
        .reset_index()
    )

    # Redondear
    agg_df["gasto_total_usd"] = agg_df["gasto_total_usd"].round(2)
    agg_df["gasto_promedio_usd"] = agg_df["gasto_promedio_usd"].round(2)

    # Subir a Gold
    minio.upload_dataframe_as_parquet(
        bucket_name="gold",
        object_name="fct_tourism_arrivals/data.parquet",
        df=agg_df,
    )

    # También cargar en DuckDB para queries directos
    duckdb.create_schema_if_not_exists("gold")
    duckdb.execute_sql(f"""
        CREATE OR REPLACE TABLE gold.fct_tourism_arrivals AS
        SELECT * FROM read_parquet('s3://gold/fct_tourism_arrivals/data.parquet');
    """)

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(agg_df)),
            "total_visitantes": MetadataValue.int(int(agg_df["total_visitantes"].sum())),
            "gasto_total_usd": MetadataValue.float(float(agg_df["gasto_total_usd"].sum())),
            "preview": MetadataValue.md(agg_df.head(10).to_markdown()),
            "minio_path": MetadataValue.text("gold/fct_tourism_arrivals/data.parquet"),
        }
    )


@asset(
    deps=["stg_hotel_occupancy"],
    description="Fact table: ocupación hotelera mensual por departamento (Gold layer).",
    metadata={
        "layer": "gold",
        "format": "parquet",
        "type": "fact_table",
    },
    compute_kind="duckdb",
)
def fct_hotel_occupancy(
    context: AssetExecutionContext,
    minio,
    duckdb,
) -> MaterializeResult:
    """
    Genera la fact table de ocupación hotelera.
    """
    df = minio.read_parquet_as_dataframe(
        bucket_name="silver",
        object_name="hotel_occupancy/occupancy_clean.parquet",
    )

    # Agregar por departamento y periodo
    agg_df = (
        df.groupby(["anio", "mes", "departamento"])
        .agg(
            avg_ocupacion=("porcentaje_ocupacion", "mean"),
            total_habitaciones_disponibles=("habitaciones_disponibles", "sum"),
            total_habitaciones_ocupadas=("habitaciones_ocupadas", "sum"),
            tarifa_promedio_cop=("tarifa_promedio_cop", "mean"),
        )
        .reset_index()
    )

    agg_df["avg_ocupacion"] = agg_df["avg_ocupacion"].round(1)
    agg_df["tarifa_promedio_cop"] = agg_df["tarifa_promedio_cop"].round(0)

    minio.upload_dataframe_as_parquet(
        bucket_name="gold",
        object_name="fct_hotel_occupancy/data.parquet",
        df=agg_df,
    )

    duckdb.create_schema_if_not_exists("gold")
    duckdb.execute_sql(f"""
        CREATE OR REPLACE TABLE gold.fct_hotel_occupancy AS
        SELECT * FROM read_parquet('s3://gold/fct_hotel_occupancy/data.parquet');
    """)

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(agg_df)),
            "avg_ocupacion_nacional": MetadataValue.float(
                float(agg_df["avg_ocupacion"].mean())
            ),
            "preview": MetadataValue.md(agg_df.head(10).to_markdown()),
        }
    )


@asset(
    description="Dimensión: catálogo de departamentos de Colombia (Gold layer).",
    metadata={
        "layer": "gold",
        "type": "dimension_table",
    },
    compute_kind="python",
)
def dim_departments(
    context: AssetExecutionContext,
    minio,
    duckdb,
) -> MaterializeResult:
    """
    Dimensión estática de departamentos de Colombia con metadata geográfica.
    """
    departments = [
        {"codigo_dane": "05", "departamento": "Antioquia", "capital": "Medellín", "region": "Andina"},
        {"codigo_dane": "08", "departamento": "Atlántico", "capital": "Barranquilla", "region": "Caribe"},
        {"codigo_dane": "11", "departamento": "Bogotá D.C.", "capital": "Bogotá", "region": "Andina"},
        {"codigo_dane": "13", "departamento": "Bolívar", "capital": "Cartagena", "region": "Caribe"},
        {"codigo_dane": "15", "departamento": "Boyacá", "capital": "Tunja", "region": "Andina"},
        {"codigo_dane": "47", "departamento": "Magdalena", "capital": "Santa Marta", "region": "Caribe"},
        {"codigo_dane": "52", "departamento": "Nariño", "capital": "Pasto", "region": "Pacífica"},
        {"codigo_dane": "63", "departamento": "Quindío", "capital": "Armenia", "region": "Andina"},
        {"codigo_dane": "66", "departamento": "Risaralda", "capital": "Pereira", "region": "Andina"},
        {"codigo_dane": "68", "departamento": "Santander", "capital": "Bucaramanga", "region": "Andina"},
        {"codigo_dane": "76", "departamento": "Valle Del Cauca", "capital": "Cali", "region": "Pacífica"},
        {"codigo_dane": "88", "departamento": "San Andrés", "capital": "San Andrés", "region": "Insular"},
    ]

    df = pd.DataFrame(departments)

    minio.upload_dataframe_as_parquet(
        bucket_name="gold",
        object_name="dim_departments/data.parquet",
        df=df,
    )

    duckdb.create_schema_if_not_exists("gold")
    duckdb.execute_sql(f"""
        CREATE OR REPLACE TABLE gold.dim_departments AS
        SELECT * FROM read_parquet('s3://gold/dim_departments/data.parquet');
    """)

    return MaterializeResult(
        metadata={
            "num_departments": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(df.to_markdown()),
        }
    )
