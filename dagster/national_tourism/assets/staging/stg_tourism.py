# =============================================================================
# Silver Layer — Staging de datos de turismo
# =============================================================================
# Este módulo toma los datos Bronze y los limpia/estandariza:
#   - Tipado de columnas
#   - Tratamiento de nulos
#   - Estandarización de nombres
#   - Validaciones básicas
# =============================================================================

import pandas as pd
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)


@asset(
    deps=["raw_tourism_arrivals"],
    description="Datos de llegadas limpiados y estandarizados (Silver layer).",
    metadata={
        "layer": "silver",
        "format": "parquet",
    },
    compute_kind="python",
)
def stg_tourism_arrivals(
    context: AssetExecutionContext,
    minio,
) -> MaterializeResult:
    """
    Limpia y estandariza los datos de llegadas de turistas.

    Transformaciones:
    - Normaliza nombres de columnas (snake_case)
    - Castea tipos de datos
    - Elimina duplicados
    - Trata valores nulos
    - Estandariza nombres de países y departamentos
    """
    # Leer datos Bronze desde MinIO
    df = minio.read_parquet_as_dataframe(
        bucket_name="bronze",
        object_name="tourism_arrivals/arrivals.parquet",
    )
    context.log.info(f"Datos Bronze leídos: {len(df)} filas")

    # --- Transformaciones Silver ---

    # 1. Normalizar nombres de columnas
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # 2. Castear fecha
    if "fecha_llegada" in df.columns:
        df["fecha_llegada"] = pd.to_datetime(df["fecha_llegada"], errors="coerce")

    # 3. Eliminar duplicados
    rows_before = len(df)
    df = df.drop_duplicates()
    duplicates_removed = rows_before - len(df)
    context.log.info(f"Duplicados eliminados: {duplicates_removed}")

    # 4. Estandarizar texto
    text_columns = ["pais_origen", "departamento_destino", "motivo_viaje", "punto_entrada"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    # 5. Tratar nulos en campos numéricos
    if "numero_visitantes" in df.columns:
        df["numero_visitantes"] = df["numero_visitantes"].fillna(0).astype(int)
    if "gasto_estimado_usd" in df.columns:
        df["gasto_estimado_usd"] = df["gasto_estimado_usd"].fillna(0.0)

    # 6. Filtrar registros inválidos
    if "numero_visitantes" in df.columns:
        df = df[df["numero_visitantes"] >= 0]

    # Subir a MinIO Silver
    minio.upload_dataframe_as_parquet(
        bucket_name="silver",
        object_name="tourism_arrivals/arrivals_clean.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "duplicates_removed": MetadataValue.int(duplicates_removed),
            "null_counts": MetadataValue.md(
                df.isnull().sum().to_frame("nulls").to_markdown()
            ),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("silver/tourism_arrivals/arrivals_clean.parquet"),
        }
    )


@asset(
    deps=["raw_hotel_occupancy"],
    description="Datos de ocupación hotelera limpiados y estandarizados (Silver layer).",
    metadata={
        "layer": "silver",
        "format": "parquet",
    },
    compute_kind="python",
)
def stg_hotel_occupancy(
    context: AssetExecutionContext,
    minio,
) -> MaterializeResult:
    """
    Limpia y estandariza los datos de ocupación hotelera.
    """
    df = minio.read_parquet_as_dataframe(
        bucket_name="bronze",
        object_name="hotel_occupancy/occupancy.parquet",
    )
    context.log.info(f"Datos Bronze leídos: {len(df)} filas")

    # Normalizar columnas
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Estandarizar departamento
    if "departamento" in df.columns:
        df["departamento"] = df["departamento"].str.strip().str.title()

    # Validar porcentaje de ocupación (0-100)
    if "porcentaje_ocupacion" in df.columns:
        df["porcentaje_ocupacion"] = df["porcentaje_ocupacion"].clip(0, 100)

    # Crear fecha a partir de año y mes
    if "anio" in df.columns and "mes" in df.columns:
        df["fecha"] = pd.to_datetime(
            df["anio"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
        )

    # Eliminar duplicados
    df = df.drop_duplicates()

    minio.upload_dataframe_as_parquet(
        bucket_name="silver",
        object_name="hotel_occupancy/occupancy_clean.parquet",
        df=df,
    )

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(df.head(5).to_markdown()),
            "minio_path": MetadataValue.text("silver/hotel_occupancy/occupancy_clean.parquet"),
        }
    )
