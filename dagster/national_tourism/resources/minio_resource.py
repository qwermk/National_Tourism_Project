# =============================================================================
# MinIO Resource — Conexión a almacenamiento S3-compatible
# =============================================================================
# Este resource encapsula la conexión a MinIO y provee métodos
# para subir/descargar archivos desde los buckets del data lake.
# =============================================================================

import os
import io
from typing import Optional

from dagster import ConfigurableResource, get_dagster_logger
from minio import Minio

logger = get_dagster_logger()


class MinIOResource(ConfigurableResource):
    """Resource para interactuar con MinIO (S3-compatible)."""

    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure: bool = False

    def _get_client(self) -> Minio:
        """Crea y retorna un cliente MinIO."""
        return Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Sube un archivo local a MinIO."""
        client = self._get_client()
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            content_type=content_type,
        )
        logger.info(f"Archivo subido: {bucket_name}/{object_name}")

    def upload_dataframe_as_parquet(
        self,
        bucket_name: str,
        object_name: str,
        df,  # pandas DataFrame
    ) -> None:
        """Sube un DataFrame de pandas como archivo Parquet a MinIO."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        client = self._get_client()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.info(f"DataFrame subido como Parquet: {bucket_name}/{object_name}")

    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
    ) -> None:
        """Descarga un archivo de MinIO al sistema local."""
        client = self._get_client()
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )
        logger.info(f"Archivo descargado: {bucket_name}/{object_name} → {file_path}")

    def read_parquet_as_dataframe(
        self,
        bucket_name: str,
        object_name: str,
    ):
        """Lee un archivo Parquet desde MinIO y lo retorna como DataFrame."""
        import pandas as pd

        client = self._get_client()
        response = client.get_object(bucket_name, object_name)
        buffer = io.BytesIO(response.read())
        response.close()
        response.release_conn()

        return pd.read_parquet(buffer)

    def list_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
    ) -> list:
        """Lista objetos en un bucket con un prefijo opcional."""
        client = self._get_client()
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Crea el bucket si no existe."""
        client = self._get_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket creado: {bucket_name}")


# ---------------------------------------------------------------------------
# Instancia por defecto para usar en Definitions
# ---------------------------------------------------------------------------
minio_resource = MinIOResource()
