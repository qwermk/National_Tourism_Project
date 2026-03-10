# =============================================================================
# DuckDB Resource — Motor de procesamiento OLAP local
# =============================================================================
# Este resource maneja la conexión a DuckDB y provee métodos
# para ejecutar queries, cargar datos y exportar resultados.
# =============================================================================

import os
from pathlib import Path

import duckdb
from dagster import ConfigurableResource, get_dagster_logger

logger = get_dagster_logger()


class DuckDBResource(ConfigurableResource):
    """Resource para interactuar con DuckDB como motor de procesamiento."""

    database_path: str = os.getenv("DUCKDB_DATABASE", "data/tourism.duckdb")

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Crea y retorna una conexión a DuckDB."""
        # Asegurar que el directorio existe
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(db_path))

        # Instalar y cargar extensiones útiles
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL parquet; LOAD parquet;")

        # Configurar acceso a MinIO via S3
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        conn.execute(f"""
            SET s3_endpoint='{minio_endpoint}';
            SET s3_access_key_id='{minio_access_key}';
            SET s3_secret_access_key='{minio_secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        return conn

    def execute_query(self, query: str):
        """Ejecuta un query SQL y retorna el resultado como DataFrame."""
        conn = self._get_connection()
        try:
            result = conn.execute(query).fetchdf()
            logger.info(f"Query ejecutado exitosamente. Filas: {len(result)}")
            return result
        finally:
            conn.close()

    def execute_sql(self, query: str) -> None:
        """Ejecuta un query SQL sin retornar resultados (CREATE, INSERT, etc.)."""
        conn = self._get_connection()
        try:
            conn.execute(query)
            logger.info("SQL ejecutado exitosamente.")
        finally:
            conn.close()

    def load_parquet_from_minio(
        self,
        bucket: str,
        path: str,
        table_name: str,
        schema: str = "main",
    ) -> None:
        """Carga un archivo Parquet desde MinIO hacia una tabla DuckDB."""
        s3_path = f"s3://{bucket}/{path}"
        query = f"""
            CREATE OR REPLACE TABLE {schema}.{table_name} AS
            SELECT * FROM read_parquet('{s3_path}');
        """
        conn = self._get_connection()
        try:
            conn.execute(query)
            count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            logger.info(f"Tabla {schema}.{table_name} cargada desde {s3_path}. Filas: {count}")
        finally:
            conn.close()

    def export_table_to_parquet(
        self,
        table_name: str,
        output_path: str,
        schema: str = "main",
    ) -> None:
        """Exporta una tabla DuckDB a un archivo Parquet."""
        query = f"""
            COPY {schema}.{table_name}
            TO '{output_path}' (FORMAT PARQUET);
        """
        conn = self._get_connection()
        try:
            conn.execute(query)
            logger.info(f"Tabla {schema}.{table_name} exportada a {output_path}")
        finally:
            conn.close()

    def create_schema_if_not_exists(self, schema: str) -> None:
        """Crea un schema en DuckDB si no existe."""
        conn = self._get_connection()
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            logger.info(f"Schema '{schema}' verificado/creado.")
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Instancia por defecto para usar en Definitions
# ---------------------------------------------------------------------------
duckdb_resource = DuckDBResource()
