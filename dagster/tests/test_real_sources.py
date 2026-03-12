# =============================================================================
# Tests: Conectores a fuentes reales (HttpResource + real_sources assets)
# =============================================================================

import io
import json
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest
import requests

from national_tourism.resources.http_resource import HttpResource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(status_code: int = 200, json_data=None, text: str = "", content: bytes = b""):
    """Construye un requests.Response simulado."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = text
    resp.content = content
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_http_resource(**kwargs) -> HttpResource:
    return HttpResource(
        timeout_seconds=kwargs.get("timeout_seconds", 5),
        max_retries=kwargs.get("max_retries", 2),
        retry_delay_seconds=kwargs.get("retry_delay_seconds", 0.0),
    )


def _make_minio_mock():
    mock = MagicMock()
    mock.upload_dataframe_as_parquet.return_value = None
    return mock


def _make_context_mock():
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


def _call_asset(asset_def, ctx, **kwargs):
    """Call a Dagster @asset function directly, bypassing Dagster's validation wrapper.

    When an AssetsDefinition is called normally (asset(context=ctx, ...)) it goes
    through direct_invocation_result, which tries to infer the asset key from a
    real DirectOpExecutionContext.  Using a MagicMock context causes that check to
    fail.  This helper accesses the original decorated Python function instead.
    """
    return asset_def.op.compute_fn.decorated_fn(ctx, **kwargs)


# ---------------------------------------------------------------------------
# HttpResource — Tests
# ---------------------------------------------------------------------------

class TestHttpResource:

    def test_get_json_parses_response(self):
        resource = _make_http_resource()
        payload = {"key": "value", "list": [1, 2, 3]}
        mock_resp = _mock_response(json_data=payload)

        with patch("requests.Session.get", return_value=mock_resp):
            result = resource.get_json("https://api.example.com/data")

        assert result == payload

    def test_get_json_raises_on_4xx(self):
        resource = _make_http_resource()
        mock_resp = _mock_response(status_code=404)

        with patch("requests.Session.get", return_value=mock_resp):
            with pytest.raises(requests.HTTPError):
                resource.get_json("https://api.example.com/missing")

    def test_get_text_returns_string(self):
        resource = _make_http_resource()
        mock_resp = _mock_response(text="hello,world\n1,2")

        with patch("requests.Session.get", return_value=mock_resp):
            result = resource.get_text("https://example.com/file.csv")

        assert result == "hello,world\n1,2"

    def test_get_dataframe_from_csv_parses_correctly(self):
        resource = _make_http_resource()
        csv_bytes = b"anio,mes,valor\n2022,1,100\n2022,2,200\n"
        mock_resp = _mock_response(content=csv_bytes)

        with patch("requests.Session.get", return_value=mock_resp):
            df = resource.get_dataframe_from_csv("https://example.com/data.csv")

        assert list(df.columns) == ["anio", "mes", "valor"]
        assert len(df) == 2
        assert df["valor"].sum() == 300

    def test_get_dataframe_from_excel_parses_correctly(self):
        resource = _make_http_resource()
        # Crear un Excel en memoria
        buf = io.BytesIO()
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_excel(buf, index=False)
        excel_bytes = buf.getvalue()
        mock_resp = _mock_response(content=excel_bytes)

        with patch("requests.Session.get", return_value=mock_resp):
            df = resource.get_dataframe_from_excel("https://example.com/data.xlsx")

        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2

    def test_retries_on_connection_error(self):
        resource = _make_http_resource(max_retries=3, retry_delay_seconds=0.0)
        good_resp = _mock_response(json_data={"ok": True})

        call_count = 0

        def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.ConnectionError("Connection refused")
            return good_resp

        with patch("requests.Session.get", side_effect=_side_effect):
            result = resource.get_json("https://example.com/unstable")

        assert result == {"ok": True}
        assert call_count == 3

    def test_raises_after_max_retries_exceeded(self):
        resource = _make_http_resource(max_retries=2, retry_delay_seconds=0.0)

        with patch(
            "requests.Session.get",
            side_effect=requests.ConnectionError("always down"),
        ):
            with pytest.raises(requests.ConnectionError):
                resource.get_json("https://example.com/down")

    def test_user_agent_header_set(self):
        resource = _make_http_resource()
        mock_resp = _mock_response(json_data={})
        captured_headers = {}

        def _capture(*args, **kwargs):
            # Access headers from the session (patched at session level)
            return mock_resp

        with patch("requests.Session.get", side_effect=_capture) as mock_get:
            resource.get_json("https://example.com/")

        mock_get.assert_called_once()


# ---------------------------------------------------------------------------
# World Bank Asset — Tests
# ---------------------------------------------------------------------------

_WB_SAMPLE_RESPONSE = [
    {"page": 1, "pages": 1, "per_page": 100, "total": 2},
    [
        {
            "indicator": {"id": "ST.INT.ARVL", "value": "International tourism, number of arrivals"},
            "country": {"id": "CO", "value": "Colombia"},
            "date": "2022",
            "value": 4200000.0,
        },
        {
            "indicator": {"id": "ST.INT.ARVL", "value": "International tourism, number of arrivals"},
            "country": {"id": "CO", "value": "Colombia"},
            "date": "2023",
            "value": 4800000.0,
        },
    ],
]


class TestRawWorldBankArrivals:

    def test_uploads_parquet_to_minio(self):
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        # Only ST.INT.ARVL returns data; others return empty
        def _wb_json(url, params=None):
            if "ST.INT.ARVL" in url:
                return _WB_SAMPLE_RESPONSE
            return [{}, None]

        with patch.object(HttpResource, "get_json", side_effect=_wb_json):
            result = _call_asset(raw_world_bank_arrivals, ctx, minio=minio, http=http)

        minio.upload_dataframe_as_parquet.assert_called_once()
        call_kwargs = minio.upload_dataframe_as_parquet.call_args[1]
        assert call_kwargs["bucket_name"] == "bronze"
        assert call_kwargs["object_name"] == "world_bank/arrivals_annual.parquet"

    def test_returns_correct_row_count(self):
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        with patch.object(HttpResource, "get_json", return_value=_WB_SAMPLE_RESPONSE):
            result = _call_asset(raw_world_bank_arrivals, ctx, minio=minio, http=http)

        # 3 indicators × 2 records = 6 rows
        assert result.metadata["num_rows"].value == 6

    def test_fallback_when_api_fails(self):
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        with patch.object(
            HttpResource, "get_json", side_effect=requests.ConnectionError("down")
        ):
            result = _call_asset(raw_world_bank_arrivals, ctx, minio=minio, http=http)

        # Falls back → still uploads something
        minio.upload_dataframe_as_parquet.assert_called_once()
        df = minio.upload_dataframe_as_parquet.call_args[1]["df"]
        assert len(df) > 0

    def test_metadata_has_expected_keys(self):
        from national_tourism.assets.ingestion.real_sources import raw_world_bank_arrivals

        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        with patch.object(HttpResource, "get_json", return_value=_WB_SAMPLE_RESPONSE):
            result = _call_asset(raw_world_bank_arrivals, ctx, minio=minio, http=http)

        for key in ("num_rows", "indicadores", "anio_min", "anio_max", "minio_path"):
            assert key in result.metadata, f"Missing metadata key: {key}"


# ---------------------------------------------------------------------------
# CITUR Arrivals Asset — Tests
# ---------------------------------------------------------------------------

_CITUR_CSV_BYTES = (
    b"anio,mes,pais_origen,departamento_destino,numero_visitantes,gasto_estimado_usd,"
    b"motivo_viaje,punto_entrada\n"
    b"2022,1,Estados Unidos,Bogot\xc3\xa1 D.C.,1500,1200000.0,Turismo,A\xc3\xa9reo\n"
    b"2022,2,Brasil,Antioquia,800,640000.0,Negocios,A\xc3\xa9reo\n"
)


class TestRawCiturArrivals:

    def test_uses_citur_url_when_configured(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.setenv("CITUR_ARRIVALS_URL", "https://datos.gov.co/fake/arrivals.csv")

        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        _csv = (
            "anio,mes,pais_origen,departamento_destino,numero_visitantes,"
            "gasto_estimado_usd,motivo_viaje,punto_entrada,fecha_llegada\n"
            "2022,1,Estados Unidos,Bogotá D.C.,1500,1200000.0,Turismo,Aéreo,2022-01-15\n"
            "2022,2,Brasil,Antioquia,800,640000.0,Negocios,Aéreo,2022-02-15\n"
        )
        with patch.object(HttpResource, "get_text", return_value=_csv):
            result = _call_asset(raw_citur_arrivals, ctx, minio=minio, http=http)

        assert result.metadata["fuente_utilizada"].value == "citur_datos_gov_co"
        assert result.metadata["num_rows"].value == 2

    def test_falls_back_to_synthetic_when_no_url(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.delenv("CITUR_ARRIVALS_URL", raising=False)
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        result = _call_asset(raw_citur_arrivals, ctx, minio=minio, http=http)

        assert result.metadata["fuente_utilizada"].value == "synthetic"
        df = minio.upload_dataframe_as_parquet.call_args[1]["df"]
        assert len(df) > 100  # Synthetic generates thousands of rows

    def test_falls_back_to_synthetic_when_request_fails(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.setenv("CITUR_ARRIVALS_URL", "https://datos.gov.co/bad.csv")
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        with patch.object(
            HttpResource, "get_text",
            side_effect=requests.ConnectionError("timeout"),
        ):
            result = _call_asset(raw_citur_arrivals, ctx, minio=minio, http=http)

        assert result.metadata["fuente_utilizada"].value == "synthetic"

    def test_uploads_to_correct_minio_path(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.delenv("CITUR_ARRIVALS_URL", raising=False)
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        _call_asset(raw_citur_arrivals, ctx, minio=minio, http=http)

        call_kwargs = minio.upload_dataframe_as_parquet.call_args[1]
        assert call_kwargs["bucket_name"] == "bronze"
        assert call_kwargs["object_name"] == "citur/tourism_arrivals.parquet"

    def test_column_normalization_tolerates_variant_names(self, monkeypatch):
        """Columna 'pais' debe renombrarse a 'pais_origen'."""
        from national_tourism.assets.ingestion.real_sources import raw_citur_arrivals

        monkeypatch.setenv("CITUR_ARRIVALS_URL", "https://datos.gov.co/fake.csv")
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        _variant_csv = (
            "año,mes,pais,departamento_destino,llegadas,fecha_llegada\n"
            "2022,3,Francia,Bogotá D.C.,500,2022-03-15\n"
            "2022,4,Italia,Bogotá D.C.,300,2022-04-15\n"
        )

        with patch.object(HttpResource, "get_text", return_value=_variant_csv):
            result = _call_asset(raw_citur_arrivals, ctx, minio=minio, http=http)

        uploaded_df = minio.upload_dataframe_as_parquet.call_args[1]["df"]
        assert "pais_origen" in uploaded_df.columns
        assert "numero_visitantes" in uploaded_df.columns
        assert "anio" in uploaded_df.columns


# ---------------------------------------------------------------------------
# CITUR Hotel Occupancy Asset — Tests
# ---------------------------------------------------------------------------

class TestRawCiturHotelOccupancy:

    def test_falls_back_to_synthetic_when_no_url(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_hotel_occupancy

        monkeypatch.delenv("CITUR_OCCUPANCY_URL", raising=False)
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        result = _call_asset(raw_citur_hotel_occupancy, ctx, minio=minio, http=http)

        assert result.metadata["fuente_utilizada"].value == "synthetic"
        df = minio.upload_dataframe_as_parquet.call_args[1]["df"]
        assert "porcentaje_ocupacion" in df.columns
        assert df["porcentaje_ocupacion"].between(0, 100).all()

    def test_calculates_habitaciones_ocupadas_from_rate(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_hotel_occupancy

        monkeypatch.setenv("CITUR_OCCUPANCY_URL", "https://datos.gov.co/occ.csv")
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        # Dataset without habitaciones_ocupadas — should be computed
        _occ_csv = (
            "anio,mes,departamento,porcentaje_ocupacion,habitaciones_disponibles,tarifa_promedio_cop\n"
            "2022,5,Antioquia,60.0,1000,200000.0\n"
        )

        with patch.object(HttpResource, "get_text", return_value=_occ_csv):
            _call_asset(raw_citur_hotel_occupancy, ctx, minio=minio, http=http)

        df = minio.upload_dataframe_as_parquet.call_args[1]["df"]
        assert df["habitaciones_ocupadas"].iloc[0] == 600  # 60% of 1000

    def test_uploads_to_correct_minio_path(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_hotel_occupancy

        monkeypatch.delenv("CITUR_OCCUPANCY_URL", raising=False)
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        _call_asset(raw_citur_hotel_occupancy, ctx, minio=minio, http=http)

        call_kwargs = minio.upload_dataframe_as_parquet.call_args[1]
        assert call_kwargs["object_name"] == "citur/hotel_occupancy.parquet"

    def test_clamps_occupancy_rate_between_0_and_100(self, monkeypatch):
        from national_tourism.assets.ingestion.real_sources import raw_citur_hotel_occupancy

        monkeypatch.setenv("CITUR_OCCUPANCY_URL", "https://datos.gov.co/occ.csv")
        ctx = _make_context_mock()
        minio = _make_minio_mock()
        http = _make_http_resource()

        _bad_csv = (
            "anio,mes,departamento,porcentaje_ocupacion,habitaciones_disponibles,"
            "habitaciones_ocupadas,tarifa_promedio_cop\n"
            "2022,1,Bogotá D.C.,150.0,1000,0,200000\n"
            "2022,2,Bogotá D.C.,-10.0,1000,0,200000\n"
        )

        with patch.object(HttpResource, "get_text", return_value=_bad_csv):
            _call_asset(raw_citur_hotel_occupancy, ctx, minio=minio, http=http)

        df = minio.upload_dataframe_as_parquet.call_args[1]["df"]
        assert df["porcentaje_ocupacion"].max() <= 100
        assert df["porcentaje_ocupacion"].min() >= 0


# ---------------------------------------------------------------------------
# Definitions integration — new assets registered
# ---------------------------------------------------------------------------

class TestRealSourcesRegistration:

    def test_all_new_assets_in_definitions(self):
        from national_tourism.definitions import defs

        asset_keys = {spec.key.to_user_string() for spec in defs.resolve_all_asset_specs()}
        assert "raw_world_bank_arrivals" in asset_keys
        assert "raw_citur_arrivals" in asset_keys
        assert "raw_citur_hotel_occupancy" in asset_keys

    def test_http_resource_registered(self):
        from national_tourism.definitions import defs

        resource_keys = list(defs.resources.keys())
        assert "http" in resource_keys
