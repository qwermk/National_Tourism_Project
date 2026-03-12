# =============================================================================
# Test: Sensor de nuevos archivos en MinIO
# =============================================================================
import pytest
from unittest.mock import MagicMock
from dagster import ResourceDefinition, build_sensor_context, RunRequest


def _run_sensor(cursor, minio_files):
    from national_tourism.sensors.new_file_sensor import new_raw_file_sensor
    mock_minio = MagicMock()
    mock_minio.list_objects.return_value = minio_files
    context = build_sensor_context(
        cursor=cursor,
        resources={"minio": ResourceDefinition.hardcoded_resource(mock_minio)},
    )
    gen = new_raw_file_sensor(context)
    results = list(gen) if gen is not None else []
    return context, results


class TestNewRawFileSensor:
    def test_first_run_with_files_triggers_run_request(self):
        _, results = _run_sensor(cursor=None, minio_files=["arrivals_2024.csv"])
        assert len(results) == 1
        assert isinstance(results[0], RunRequest)

    def test_first_run_empty_bucket_no_request(self):
        _, results = _run_sensor(cursor=None, minio_files=[])
        assert len(results) == 0

    def test_no_new_files_since_last_run(self):
        _, results = _run_sensor(cursor="arrivals_2024.csv", minio_files=["arrivals_2024.csv"])
        assert len(results) == 0

    def test_new_file_added_triggers_run_request(self):
        _, results = _run_sensor(
            cursor="arrivals_2023.csv",
            minio_files=["arrivals_2023.csv", "arrivals_2024.csv"],
        )
        assert len(results) == 1

    def test_cursor_updated_after_new_files(self):
        context, _ = _run_sensor(
            cursor="arrivals_2023.csv",
            minio_files=["arrivals_2023.csv", "arrivals_2024.csv"],
        )
        assert context.cursor is not None
        assert "arrivals_2023.csv" in context.cursor
        assert "arrivals_2024.csv" in context.cursor

    def test_cursor_not_updated_when_no_new_files(self):
        original = "arrivals_2023.csv"
        context, _ = _run_sensor(cursor=original, minio_files=["arrivals_2023.csv"])
        assert context.cursor == original

    def test_minio_error_does_not_raise(self):
        from national_tourism.sensors.new_file_sensor import new_raw_file_sensor
        mock_minio = MagicMock()
        mock_minio.list_objects.side_effect = ConnectionError("MinIO no disponible")
        context = build_sensor_context(
            cursor=None,
            resources={"minio": ResourceDefinition.hardcoded_resource(mock_minio)},
        )
        gen = new_raw_file_sensor(context)
        results = list(gen) if gen is not None else []
        assert results == []

    def test_run_request_has_run_key(self):
        _, results = _run_sensor(cursor=None, minio_files=["arrivals_2024.csv"])
        assert len(results) == 1
        assert results[0].run_key is not None
        assert len(results[0].run_key) > 0

    def test_multiple_new_files_single_run_request(self):
        _, results = _run_sensor(
            cursor=None,
            minio_files=["file_a.csv", "file_b.csv", "file_c.csv"],
        )
        assert len(results) == 1
