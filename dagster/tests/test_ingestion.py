# =============================================================================
# Test: Assets de ingesta (Bronze layer)
# =============================================================================
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from dagster import build_asset_context


@pytest.fixture
def mock_minio():
    minio = MagicMock()
    minio.upload_dataframe_as_parquet = MagicMock()
    minio.ensure_bucket_exists = MagicMock()
    return minio


@pytest.fixture
def asset_context():
    return build_asset_context()


class TestRawTourismArrivals:
    def test_fallback_synthetic_data_generated(self, asset_context, mock_minio):
        from national_tourism.assets.ingestion.tourism_arrivals import raw_tourism_arrivals
        with patch("national_tourism.assets.ingestion.tourism_arrivals.Path") as mock_path:
            mock_path.return_value.glob.return_value = []
            result = raw_tourism_arrivals(asset_context, mock_minio)
        mock_minio.upload_dataframe_as_parquet.assert_called_once()
        call_kwargs = mock_minio.upload_dataframe_as_parquet.call_args
        assert call_kwargs.kwargs["bucket_name"] == "bronze"
        assert call_kwargs.kwargs["object_name"] == "tourism_arrivals/arrivals.parquet"

    def test_synthetic_data_has_required_columns(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_arrivals_data
        df = _create_sample_arrivals_data()
        required = {"anio", "mes", "pais_origen", "departamento_destino", "numero_visitantes", "gasto_estimado_usd"}
        assert not (required - set(df.columns))

    def test_synthetic_data_no_null_visitors(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_arrivals_data
        df = _create_sample_arrivals_data()
        assert df["numero_visitantes"].isna().sum() == 0

    def test_synthetic_data_positive_visitors(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_arrivals_data
        df = _create_sample_arrivals_data()
        assert (df["numero_visitantes"] >= 0).all()

    def test_synthetic_data_valid_year_range(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_arrivals_data
        df = _create_sample_arrivals_data()
        assert df["anio"].min() >= 2019
        assert df["anio"].max() <= 2024

    def test_synthetic_data_valid_months(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_arrivals_data
        df = _create_sample_arrivals_data()
        assert df["mes"].between(1, 12).all()

    def test_materialise_result_has_metadata(self, asset_context, mock_minio):
        from national_tourism.assets.ingestion.tourism_arrivals import raw_tourism_arrivals
        with patch("national_tourism.assets.ingestion.tourism_arrivals.Path") as mock_path:
            mock_path.return_value.glob.return_value = []
            result = raw_tourism_arrivals(asset_context, mock_minio)
        assert result is not None
        assert "num_rows" in result.metadata
        assert "bronze_path" in result.metadata


class TestRawHotelOccupancy:
    def test_synthetic_data_has_required_columns(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_occupancy_data
        df = _create_sample_occupancy_data()
        required = {"anio", "mes", "departamento", "porcentaje_ocupacion",
                    "habitaciones_disponibles", "habitaciones_ocupadas", "tarifa_promedio_cop"}
        assert not (required - set(df.columns))

    def test_occupancy_rate_in_valid_range(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_occupancy_data
        df = _create_sample_occupancy_data()
        assert df["porcentaje_ocupacion"].between(0, 100).all()

    def test_rooms_occupied_le_available(self):
        from national_tourism.assets.ingestion.tourism_arrivals import _create_sample_occupancy_data
        df = _create_sample_occupancy_data()
        assert (df["habitaciones_ocupadas"] <= df["habitaciones_disponibles"]).all()

    def test_upload_called_to_correct_bucket(self, asset_context, mock_minio):
        from national_tourism.assets.ingestion.tourism_arrivals import raw_hotel_occupancy
        with patch("national_tourism.assets.ingestion.tourism_arrivals.Path") as mock_path:
            mock_path.return_value.glob.return_value = []
            raw_hotel_occupancy(asset_context, mock_minio)
        mock_minio.upload_dataframe_as_parquet.assert_called_once()
        call_kwargs = mock_minio.upload_dataframe_as_parquet.call_args
        assert call_kwargs.kwargs["bucket_name"] == "bronze"
        assert call_kwargs.kwargs["object_name"] == "hotel_occupancy/occupancy.parquet"
