# =============================================================================
# Test: Sensor de alertas de fallos del pipeline
# =============================================================================

import json
import os
import smtplib
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: argumentos de fallo simulados para _process_failure_alert
# ---------------------------------------------------------------------------

def _failure_kwargs(
    job_name="daily_tourism_pipeline",
    run_id=None,
    tags=None,
    error_message="Op 'raw_tourism_arrivals' failed: Connection refused",
):
    """Devuelve los kwargs para llamar _process_failure_alert directamente."""
    return dict(
        run_id=run_id or "abcdef12-0000-0000-0000-000000000000",
        job_name=job_name,
        tags=tags or {},
        error_message=error_message,
        log=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Tests de helpers internos
# ---------------------------------------------------------------------------

class TestSendSlackAlert:
    def test_raises_on_non_200(self):
        """_send_slack_alert debe lanzar RuntimeError si el webhook no devuelve 200."""
        from national_tourism.sensors.alert_sensor import _send_slack_alert

        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 400

        with patch("urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="HTTP 400"):
                _send_slack_alert("https://hooks.slack.com/fake", "test")

    def test_sends_json_body(self):
        """_send_slack_alert debe enviar JSON con la clave 'text'."""
        from national_tourism.sensors.alert_sensor import _send_slack_alert

        captured = {}

        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200

        def fake_urlopen(req, timeout=None):
            captured["data"] = req.data
            return mock_resp

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            _send_slack_alert("https://hooks.slack.com/fake", "hola mundo")

        payload = json.loads(captured["data"].decode())
        assert payload["text"] == "hola mundo"


class TestSendEmailAlert:
    def test_no_op_when_smtp_host_missing(self):
        """_send_email_alert debe ser un no-op si SMTP_HOST no está definido."""
        from national_tourism.sensors.alert_sensor import _send_email_alert

        with patch("smtplib.SMTP") as mock_smtp:
            _send_email_alert("Asunto", "Cuerpo")
        mock_smtp.assert_not_called()

    def test_calls_smtp_when_configured(self, monkeypatch):
        """_send_email_alert debe conectar al SMTP si todas las vars están definidas."""
        from national_tourism.sensors.alert_sensor import _send_email_alert

        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.setenv("SMTP_PORT", "587")
        monkeypatch.setenv("SMTP_USER", "user@example.com")
        monkeypatch.setenv("SMTP_PASSWORD", "secret")
        monkeypatch.setenv("ALERT_EMAIL_FROM", "alerts@example.com")
        monkeypatch.setenv("ALERT_EMAIL_TO", "admin@example.com")

        mock_server = MagicMock()
        mock_server.__enter__ = lambda s: s
        mock_server.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP", return_value=mock_server):
            _send_email_alert("Asunto test", "Cuerpo test")

        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user@example.com", "secret")
        mock_server.sendmail.assert_called_once()
        call_args = mock_server.sendmail.call_args
        assert call_args[0][0] == "alerts@example.com"
        assert call_args[0][1] == ["admin@example.com"]


# ---------------------------------------------------------------------------
# Tests del sensor principal
# ---------------------------------------------------------------------------

class TestProcessFailureAlert:
    """Tests de la lógica central _process_failure_alert (sin contexto Dagster real)."""

    def test_logs_failure_always(self):
        """_process_failure_alert debe registrar el fallo en el log siempre."""
        from national_tourism.sensors.alert_sensor import _process_failure_alert

        kwargs = _failure_kwargs()
        log = kwargs["log"]

        os.environ.pop("SLACK_WEBHOOK_URL", None)
        os.environ.pop("SMTP_HOST", None)
        _process_failure_alert(**kwargs)

        log.warning.assert_called_once()
        warning_msg = log.warning.call_args[0][0]
        assert "daily_tourism_pipeline" in warning_msg

    def test_slack_called_when_webhook_set(self, monkeypatch):
        """Debe llamar a _send_slack_alert si SLACK_WEBHOOK_URL está definido."""
        from national_tourism.sensors.alert_sensor import _process_failure_alert

        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        kwargs = _failure_kwargs(job_name="test_job")

        with patch(
            "national_tourism.sensors.alert_sensor._send_slack_alert"
        ) as mock_slack:
            _process_failure_alert(**kwargs)

        mock_slack.assert_called_once()
        message = mock_slack.call_args[0][1]
        assert "test_job" in message
        assert "red_circle" in message

    def test_slack_error_does_not_raise(self, monkeypatch):
        """Si Slack falla, no debe propagarse la excepción."""
        from national_tourism.sensors.alert_sensor import _process_failure_alert

        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/bad")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        kwargs = _failure_kwargs()
        log = kwargs["log"]

        with patch(
            "national_tourism.sensors.alert_sensor._send_slack_alert",
            side_effect=RuntimeError("Slack down"),
        ):
            _process_failure_alert(**kwargs)  # No debe lanzar

        log.error.assert_called_once()
        assert "Slack" in log.error.call_args[0][0]

    def test_email_called_when_smtp_set(self, monkeypatch):
        """Debe llamar a _send_email_alert si SMTP_HOST está definido."""
        from national_tourism.sensors.alert_sensor import _process_failure_alert

        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

        kwargs = _failure_kwargs(job_name="daily_tourism_pipeline")

        with patch(
            "national_tourism.sensors.alert_sensor._send_email_alert"
        ) as mock_email:
            _process_failure_alert(**kwargs)

        mock_email.assert_called_once()
        subject, body = mock_email.call_args[0]
        assert "daily_tourism_pipeline" in subject
        assert "ALERTA" in subject
        assert "daily_tourism_pipeline" in body

    def test_email_error_does_not_raise(self, monkeypatch):
        """Si el email falla, no debe propagarse la excepción."""
        from national_tourism.sensors.alert_sensor import _process_failure_alert

        monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

        kwargs = _failure_kwargs()
        log = kwargs["log"]

        with patch(
            "national_tourism.sensors.alert_sensor._send_email_alert",
            side_effect=smtplib.SMTPException("SMTP error"),
        ):
            _process_failure_alert(**kwargs)  # No debe lanzar

        log.error.assert_called_once()

    def test_dagster_ui_url_uses_env_var(self, monkeypatch):
        """La URL de Dagster en el mensaje Slack debe respetar DAGSTER_UI_URL."""
        from national_tourism.sensors.alert_sensor import _process_failure_alert

        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
        monkeypatch.setenv("DAGSTER_UI_URL", "https://dagster.mycompany.com")
        monkeypatch.delenv("SMTP_HOST", raising=False)

        kwargs = _failure_kwargs(run_id="run-uuid-1234")

        with patch(
            "national_tourism.sensors.alert_sensor._send_slack_alert"
        ) as mock_slack:
            _process_failure_alert(**kwargs)

        message = mock_slack.call_args[0][1]
        assert "dagster.mycompany.com" in message

    def test_sensor_registered_in_definitions(self):
        """pipeline_failure_alert debe estar registrado en las Definitions."""
        from national_tourism.definitions import defs

        sensor_names = [s.name for s in defs.sensors]
        assert "pipeline_failure_alert" in sensor_names

    def test_sensor_default_status_is_running(self):
        """El sensor de alertas debe estar RUNNING por defecto."""
        from dagster import DefaultSensorStatus
        from national_tourism.sensors.alert_sensor import pipeline_failure_alert

        assert pipeline_failure_alert.default_status == DefaultSensorStatus.RUNNING
