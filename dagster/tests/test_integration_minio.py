# =============================================================================
# Integration Tests — Dagster materializa assets y guarda datos en MinIO
# =============================================================================
# Estos tests requieren MinIO corriendo en localhost:9000.
#
# Ejecución:
#   pytest tests/test_integration_minio.py -v -m integration
#
# Para excluirlos de la suite rápida (unit tests):
#   pytest tests/ -m "not integration"
#
# Variables esperadas (valores por defecto Docker Compose):
#   MINIO_ENDPOINT   localhost:9000
#   MINIO_ACCESS_KEY minioadmin
#   MINIO_SECRET_KEY minioadmin
# =============================================================================

import io
import os

import pandas as pd
import pytest
from minio import Minio
from minio.error import S3Error
from dagster import materialize

from national_tourism.resources.minio_resource import MinIOResource
from national_tourism.resources.http_resource import HttpResource

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def minio_client():
    """Cliente directo a MinIO. Salta toda la sesión si el servicio no responde."""
    client = Minio(_ENDPOINT, access_key=_ACCESS, secret_key=_SECRET, secure=False)
    try:
        client.list_buckets()
    except Exception as exc:
        pytest.skip(f"MinIO no accesible en {_ENDPOINT}: {exc}")
    return client


@pytest.fixture(scope="session")
def minio_res():
    """MinIOResource configurado para la instancia local."""
    return MinIOResource(endpoint=_ENDPOINT, access_key=_ACCESS, secret_key=_SECRET)


@pytest.fixture(scope="session")
def http_res():
    """HttpResource con timeout razonable para tests de integración."""
    return HttpResource(timeout_seconds=30, max_retries=2, retry_delay_seconds=1.0)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _read_parquet(client: Minio, bucket: str, key: str) -> pd.DataFrame:
    """Descarga y deserializa un parquet de MinIO como DataFrame."""
    response = client.get_object(bucket, key)
    try:
        buf = io.BytesIO(response.read())
    finally:
        response.close()
        response.release_conn()
    return pd.read_parquet(buf)


def _object_exists(client: Minio, bucket: str, key: str) -> bool:
    """Retorna True si el objeto existe en el bucket."""
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error:
        return False


# ===========================================================================
# Clase 1 — Infraestructura MinIO
# ===========================================================================


@pytest.mark.integration
class TestMinIOInfrastructura:
    """Verifica que MinIO está listo y los buckets del data lake existen."""

    def test_buckets_del_medallion_existen(self, minio_client):
        buckets = {b.name for b in minio_client.list_buckets()}
        for requerido in ("bronze", "silver", "gold", "raw"):
            assert requerido in buckets, f"Bucket '{requerido}' no encontrado en MinIO"

    def test_write_read_parquet_round_trip(self, minio_client, minio_res):
        """Escribe un parquet vía MinIOResource y lo lee de vuelta."""
        df_original = pd.DataFrame({"id": [1, 2, 3], "valor": ["a", "b", "c"]})
        key = "test/_integration_roundtrip.parquet"

        minio_res.upload_dataframe_as_parquet(
            bucket_name="bronze",
            object_name=key,
            df=df_original,
        )

        df_leido = _read_parquet(minio_client, "bronze", key)

        assert list(df_leido.columns) == ["id", "valor"]
        assert len(df_leido) == 3
        assert df_leido["id"].tolist() == [1, 2, 3]

        # limpieza
        minio_client.remove_object("bronze", key)


# ===========================================================================
# Clase 2 — Dagster materializa y almacena en MinIO
# ===========================================================================


@pytest.mark.integration
class TestDagsterMaterializaEnMinIO:
    """Dagster ejecuta los assets y los parquet quedan guardados en MinIO."""

    def test_raw_tourism_arrivals_escribe_parquet(self, minio_client, minio_res):
        """Asset existente: raw_tourism_arrivals → bronze/tourism_arrivals/arrivals.parquet"""
        from national_tourism.assets.ingestion.tourism_arrivals import raw_tourism_arrivals

        result = materialize(
            [raw_tourism_arrivals],
            resources={"minio": minio_res},
        )
        assert result.success, "La materialización de raw_tourism_arrivals falló"

        assert _object_exists(minio_client, "bronze", "tourism_arrivals/arrivals.parquet"), \
            "El parquet no fue creado en bronze/tourism_arrivals/arrivals.parquet"

        df = _read_parquet(minio_client, "bronze", "tourism_arrivals/arrivals.parquet")
        assert len(df) > 0, "El parquet está vacío"

    def test_raw_hotel_occupancy_escribe_parquet(self, minio_client, minio_res):
        """Asset existente: raw_hotel_occupancy → bronze/hotel_occupancy/occupancy.parquet"""
        from national_tourism.assets.ingestion.tourism_arrivals import raw_hotel_occupancy

        result = materialize(
            [raw_hotel_occupancy],
            resources={"minio": minio_res},
        )
        assert result.success, "La materialización de raw_hotel_occupancy falló"

        assert _object_exists(minio_client, "bronze", "hotel_occupancy/occupancy.parquet"), \
            "El parquet no fue creado en bronze/hotel_occupancy/occupancy.parquet"

        df = _read_parquet(minio_client, "bronze", "hotel_occupancy/occupancy.parquet")
        assert len(df) > 0

    def test_raw_world_bank_arrivals_escribe_parquet(self, minio_client, minio_res, http_res):
        """raw_world_bank_arrivals llama a World Bank API (o fallback) y escribe en MinIO."""
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        result = materialize(
            [raw_world_bank_arrivals],
            resources={"minio": minio_res, "http": http_res},
        )
        assert result.success, "La materialización de raw_world_bank_arrivals falló"

        assert _object_exists(minio_client, "bronze", "world_bank/arrivals_annual.parquet"), \
            "El parquet no fue creado en bronze/world_bank/arrivals_annual.parquet"

        df = _read_parquet(minio_client, "bronze", "world_bank/arrivals_annual.parquet")
        assert len(df) > 0
        # Esquema mínimo esperado
        for col in ("year", "indicator_code", "value"):
            assert col in df.columns, f"Columna '{col}' faltante en el parquet"

    def test_raw_citur_arrivals_synthetic_escribe_parquet(
        self, minio_client, minio_res, http_res, monkeypatch
    ):
        """Sin CITUR_ARRIVALS_URL → fallback sintético → bronze/citur/tourism_arrivals.parquet"""
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.delenv("CITUR_ARRIVALS_URL", raising=False)

        result = materialize(
            [raw_citur_arrivals],
            resources={"minio": minio_res, "http": http_res},
        )
        assert result.success, "La materialización de raw_citur_arrivals falló"

        assert _object_exists(minio_client, "bronze", "citur/tourism_arrivals.parquet"), \
            "El parquet no fue creado en bronze/citur/tourism_arrivals.parquet"

        df = _read_parquet(minio_client, "bronze", "citur/tourism_arrivals.parquet")
        assert len(df) > 100, "Se esperaban > 100 filas del dataset sintético"
        for col in ("year", "month", "number_of_visitors"):
            assert col in df.columns, f"Columna '{col}' faltante"

    def test_raw_citur_hotel_occupancy_synthetic_escribe_parquet(
        self, minio_client, minio_res, http_res, monkeypatch
    ):
        """Sin CITUR_OCCUPANCY_URL → fallback sintético → bronze/citur/hotel_occupancy.parquet"""
        from national_tourism.assets.ingestion.real_sources import raw_citur_hotel_occupancy

        monkeypatch.delenv("CITUR_OCCUPANCY_URL", raising=False)

        result = materialize(
            [raw_citur_hotel_occupancy],
            resources={"minio": minio_res, "http": http_res},
        )
        assert result.success, "La materialización de raw_citur_hotel_occupancy falló"

        assert _object_exists(minio_client, "bronze", "citur/hotel_occupancy.parquet"), \
            "El parquet no fue creado en bronze/citur/hotel_occupancy.parquet"

        df = _read_parquet(minio_client, "bronze", "citur/hotel_occupancy.parquet")
        assert len(df) > 0
        assert "occupancy_rate" in df.columns


# ===========================================================================
# Clase 3 — Calidad de los datos almacenados
# ===========================================================================


@pytest.mark.integration
class TestCalidadDatosAlmacenados:
    """Verifica integridad de los parquet que Dagster dejó en MinIO."""

    def test_world_bank_anio_minimo(self, minio_client, minio_res, http_res):
        """Todos los registros del World Bank deben tener year >= 2010."""
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        materialize(
            [raw_world_bank_arrivals],
            resources={"minio": minio_res, "http": http_res},
        )
        df = _read_parquet(minio_client, "bronze", "world_bank/arrivals_annual.parquet")

        assert (df["year"] >= 2010).all(), \
            f"Registros con year < 2010: {df[df['year'] < 2010]}"

    def test_world_bank_indicadores_validos(self, minio_client, minio_res, http_res):
        """Solo deben aparecer los tres indicadores configurados."""
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        materialize(
            [raw_world_bank_arrivals],
            resources={"minio": minio_res, "http": http_res},
        )
        df = _read_parquet(minio_client, "bronze", "world_bank/arrivals_annual.parquet")

        valid = {"ST.INT.ARVL", "ST.INT.DPRT", "ST.INT.RCPT.CD"}
        found = set(df["indicator_code"].unique())
        assert found.issubset(valid), f"Indicadores inesperados: {found - valid}"

    def test_citur_arrivals_sin_nulos_en_columnas_clave(
        self, minio_client, minio_res, http_res, monkeypatch
    ):
        """Columnas clave del dataset sintético no deben tener nulos."""
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.delenv("CITUR_ARRIVALS_URL", raising=False)
        materialize(
            [raw_citur_arrivals],
            resources={"minio": minio_res, "http": http_res},
        )
        df = _read_parquet(minio_client, "bronze", "citur/tourism_arrivals.parquet")

        for col in ("year", "month", "number_of_visitors"):
            nulls = df[col].isna().sum()
            assert nulls == 0, f"Columna '{col}' tiene {nulls} nulos"

    def test_hotel_occupancy_tasa_entre_0_y_100(
        self, minio_client, minio_res, http_res, monkeypatch
    ):
        """occupancy_rate debe estar acotado entre 0 y 100."""
        from national_tourism.assets.ingestion.real_sources import raw_citur_hotel_occupancy

        monkeypatch.delenv("CITUR_OCCUPANCY_URL", raising=False)
        materialize(
            [raw_citur_hotel_occupancy],
            resources={"minio": minio_res, "http": http_res},
        )
        df = _read_parquet(minio_client, "bronze", "citur/hotel_occupancy.parquet")

        assert df["occupancy_rate"].between(0, 100).all(), \
            "occupancy_rate fuera del rango [0, 100]"
        assert (df["occupied_rooms"] >= 0).all(), \
            "occupied_rooms con valores negativos"

    def test_todos_los_parquets_tienen_datos(self, minio_client):
        """Todos los parquets escritos por las tests anteriores deben tener datos."""
        archivos = [
            ("bronze", "tourism_arrivals/arrivals.parquet"),
            ("bronze", "hotel_occupancy/occupancy.parquet"),
            ("bronze", "world_bank/arrivals_annual.parquet"),
            ("bronze", "citur/tourism_arrivals.parquet"),
            ("bronze", "citur/hotel_occupancy.parquet"),
        ]
        for bucket, key in archivos:
            assert _object_exists(minio_client, bucket, key), \
                f"Archivo faltante: {bucket}/{key}"
            df = _read_parquet(minio_client, bucket, key)
            assert len(df) > 0, f"Parquet vacío: {bucket}/{key}"
