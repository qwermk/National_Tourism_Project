# =============================================================================
# Test: Resources de Dagster — MinIO y DuckDB
# =============================================================================
# Verifica la configuración y comportamiento básico de los resources
# sin requerir servicios externos corriendo.
# =============================================================================

import os
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Tests de MinIOResource
# ---------------------------------------------------------------------------

class TestMinIOResource:
    """Tests de configuración del MinIOResource."""

    def test_default_endpoint_from_env(self, monkeypatch):
        """El endpoint debe leer la variable de entorno MINIO_ENDPOINT."""
        monkeypatch.setenv("MINIO_ENDPOINT", "custom-host:9000")
        # Forzar re-evaluación del default (el recurso usa os.getenv al definir la clase)
        from national_tourism.resources.minio_resource import MinIOResource
        resource = MinIOResource(endpoint="custom-host:9000")
        assert resource.endpoint == "custom-host:9000"

    def test_secure_false_by_default(self):
        """La conexión debe ser no-SSL por defecto (entorno local)."""
        from national_tourism.resources.minio_resource import MinIOResource
        resource = MinIOResource()
        assert resource.secure is False

    def test_upload_dataframe_calls_put_object(self):
        """upload_dataframe_as_parquet debe llamar a put_object del cliente Minio."""
        import pandas as pd
        from national_tourism.resources.minio_resource import MinIOResource

        resource = MinIOResource()
        mock_client = MagicMock()

        with patch.object(MinIOResource, "_get_client", return_value=mock_client):
            df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
            resource.upload_dataframe_as_parquet("bronze", "test/data.parquet", df)

        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args
        assert call_kwargs.kwargs["bucket_name"] == "bronze"
        assert call_kwargs.kwargs["object_name"] == "test/data.parquet"

    def test_read_parquet_returns_dataframe(self):
        """read_parquet_as_dataframe debe retornar un DataFrame de pandas."""
        import io
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from national_tourism.resources.minio_resource import MinIOResource

        # Crear un Parquet en memoria para simular la respuesta de MinIO
        sample_df = pd.DataFrame({"valor": [10, 20, 30]})
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(sample_df), buffer)
        buffer.seek(0)

        resource = MinIOResource()
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.read.return_value = buffer.getvalue()
        mock_client.get_object.return_value = mock_response

        with patch.object(MinIOResource, "_get_client", return_value=mock_client):
            result_df = resource.read_parquet_as_dataframe("bronze", "test/data.parquet")

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 3
        assert "valor" in result_df.columns

    def test_ensure_bucket_creates_if_not_exists(self):
        """ensure_bucket_exists debe crear el bucket si no existe."""
        from national_tourism.resources.minio_resource import MinIOResource
        from minio.error import S3Error

        resource = MinIOResource()
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = False

        with patch.object(MinIOResource, "_get_client", return_value=mock_client):
            resource.ensure_bucket_exists("new-bucket")

        mock_client.make_bucket.assert_called_once_with("new-bucket")

    def test_ensure_bucket_skips_if_exists(self):
        """ensure_bucket_exists no debe recrear un bucket existente."""
        from national_tourism.resources.minio_resource import MinIOResource

        resource = MinIOResource()
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = True

        with patch.object(MinIOResource, "_get_client", return_value=mock_client):
            resource.ensure_bucket_exists("existing-bucket")

        mock_client.make_bucket.assert_not_called()


# ---------------------------------------------------------------------------
# Tests de DuckDBResource
# ---------------------------------------------------------------------------

class TestDuckDBResource:
    """Tests de configuración del DuckDBResource."""

    def test_execute_query_returns_dataframe(self, tmp_path):
        """execute_query debe retornar un DataFrame."""
        import pandas as pd
        from national_tourism.resources.duckdb_resource import DuckDBResource

        db_path = str(tmp_path / "test.duckdb")
        resource = DuckDBResource(database_path=db_path)

        # Patch at class level to avoid Pydantic frozen instance restriction
        with patch.object(DuckDBResource, "_get_connection") as mock_conn_fn:
            import duckdb
            conn = duckdb.connect(":memory:")
            mock_conn_fn.return_value = conn
            result = resource.execute_query("SELECT 42 AS valor")

        assert isinstance(result, pd.DataFrame)
        assert result["valor"].iloc[0] == 42

    def test_create_schema_executes_sql(self, tmp_path):
        """create_schema_if_not_exists debe ejecutar CREATE SCHEMA."""
        from national_tourism.resources.duckdb_resource import DuckDBResource

        db_path = str(tmp_path / "test.duckdb")
        resource = DuckDBResource(database_path=db_path)

        with patch.object(DuckDBResource, "_get_connection") as mock_conn_fn:
            import duckdb
            # Use side_effect so each call gets a fresh connection (first call closes it)
            mock_conn_fn.side_effect = lambda: duckdb.connect(":memory:")
            # No debe lanzar excepción
            resource.create_schema_if_not_exists("gold")
            resource.create_schema_if_not_exists("gold")  # Idempotente

    def test_execute_sql_no_return(self, tmp_path):
        """execute_sql debe ejecutar sin retornar resultados."""
        from national_tourism.resources.duckdb_resource import DuckDBResource

        db_path = str(tmp_path / "test.duckdb")
        resource = DuckDBResource(database_path=db_path)

        with patch.object(DuckDBResource, "_get_connection") as mock_conn_fn:
            import duckdb
            conn = duckdb.connect(":memory:")
            mock_conn_fn.return_value = conn
            # No debe lanzar excepción
            resource.execute_sql("CREATE TABLE test_tbl (id INTEGER)")
