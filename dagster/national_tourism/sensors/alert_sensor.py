# =============================================================================
# Alert Sensor — Notificaciones de fallos del pipeline
# =============================================================================
# Detecta cualquier run de Dagster que falle y envía alertas via:
#   - Slack  (configura SLACK_WEBHOOK_URL)
#   - Email  (configura SMTP_HOST + credenciales)
#
# Ambos canales son opcionales: si la variable de entorno no está definida,
# el canal simplemente se omite sin error.  El sensor siempre registra el
# fallo en el log de Dagster independientemente de la configuración.
#
# Activación: RUNNING por defecto (activo desde el primer `dagster dev`).
# Desactivar desde la UI de Dagster si se desea silenciar alertas.
# =============================================================================

import json
import os
import smtplib
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor


# ---------------------------------------------------------------------------
# Helpers de notificación
# ---------------------------------------------------------------------------

def _send_slack_alert(webhook_url: str, message: str) -> None:
    """Envía un mensaje a Slack via Incoming Webhook (no requiere dependencias extra)."""
    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
        if resp.status != 200:
            raise RuntimeError(f"Slack webhook devolvió HTTP {resp.status}")


def _send_email_alert(subject: str, body: str) -> None:
    """
    Envía un email de alerta via SMTP con STARTTLS.

    Variables de entorno requeridas:
        SMTP_HOST, SMTP_PORT (default: 587), SMTP_USER, SMTP_PASSWORD,
        ALERT_EMAIL_FROM, ALERT_EMAIL_TO
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("ALERT_EMAIL_FROM")
    email_to = os.getenv("ALERT_EMAIL_TO")

    # Silencio si la configuración está incompleta
    if not all([smtp_host, smtp_user, smtp_password, email_from, email_to]):
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [email_to], msg.as_string())


def _build_dagster_ui_url(run_id: str) -> str:
    """Construye la URL de la UI de Dagster para el run."""
    base = os.getenv("DAGSTER_UI_URL", "http://localhost:3000")
    return f"{base.rstrip('/')}/runs/{run_id}"


def _process_failure_alert(
    run_id: str,
    job_name: str,
    tags: dict,
    error_message: str,
    log,
) -> None:
    """
    Lógica central de despacho de alertas.

    Extraída del sensor para permitir pruebas unitarias sin necesidad de
    un contexto real de Dagster (RunFailureSensorContext).
    """
    run_id_short = run_id[:8]
    dagster_url = _build_dagster_ui_url(run_id)

    tags_str = ", ".join(
        f"{k}={v}"
        for k, v in (tags or {}).items()
        if not k.startswith(".dagster/")
    ) or "—"

    # ------------------------------------------------------------------
    # Mensaje Slack (Markdown de Slack)
    # ------------------------------------------------------------------
    slack_message = (
        f":red_circle: *Pipeline fallido* — `{job_name}`\n"
        f">*Run ID:* `{run_id_short}...`\n"
        f">*Error:* {error_message}\n"
        f">*Tags:* {tags_str}\n"
        f">*Detalles:* {dagster_url}"
    )

    # ------------------------------------------------------------------
    # Mensaje Email (texto plano)
    # ------------------------------------------------------------------
    email_subject = f"[ALERTA] Pipeline fallido: {job_name} (run {run_id_short})"
    email_body = "\n".join([
        "Se ha detectado un fallo en el pipeline de turismo.",
        "",
        f"Pipeline : {job_name}",
        f"Run ID   : {run_id}",
        f"Error    : {error_message}",
        f"Tags     : {tags_str}",
        f"URL      : {dagster_url}",
        "",
        "---",
        "National Tourism Pipeline — Dagster OSS",
    ])

    # ------------------------------------------------------------------
    # Enviar a Slack
    # ------------------------------------------------------------------
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    slack_ok = False
    if slack_webhook:
        try:
            _send_slack_alert(slack_webhook, slack_message)
            slack_ok = True
            log.info("Alerta Slack enviada correctamente.")
        except Exception as exc:
            log.error(f"Error al enviar alerta Slack: {exc}")

    # ------------------------------------------------------------------
    # Enviar por Email
    # ------------------------------------------------------------------
    smtp_host = os.getenv("SMTP_HOST")
    email_ok = False
    if smtp_host:
        try:
            _send_email_alert(email_subject, email_body)
            email_ok = True
            log.info(f"Alerta email enviada a {os.getenv('ALERT_EMAIL_TO')}.")
        except Exception as exc:
            log.error(f"Error al enviar alerta email: {exc}")

    # ------------------------------------------------------------------
    # Log siempre (independiente de los canales configurados)
    # ------------------------------------------------------------------
    log.warning(
        f"Run fallido: job='{job_name}' run_id='{run_id}' | "
        f"Slack={'✓' if slack_ok else ('✗ error' if slack_webhook else 'no configurado')} | "
        f"Email={'✓' if email_ok else ('✗ error' if smtp_host else 'no configurado')} | "
        f"URL: {dagster_url}"
    )


# ---------------------------------------------------------------------------
# Sensor principal
# ---------------------------------------------------------------------------

@run_failure_sensor(
    name="pipeline_failure_alert",
    description=(
        "Notifica via Slack y/o email cuando cualquier run de Dagster falla. "
        "Configura SLACK_WEBHOOK_URL y/o SMTP_HOST para activar cada canal."
    ),
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
def pipeline_failure_alert(context: RunFailureSensorContext) -> None:
    """
    Detecta runs fallidos y envía notificaciones a los canales configurados.

    Canales (variables de entorno):
      Slack:  SLACK_WEBHOOK_URL
      Email:  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
              ALERT_EMAIL_FROM, ALERT_EMAIL_TO

    Variable opcional:
      DAGSTER_UI_URL  — URL base de la UI (default: http://localhost:3000)
    """
    run = context.dagster_run
    failure_event = context.failure_event

    error_message = "Error desconocido"
    if failure_event and failure_event.message:
        error_message = failure_event.message[:600]

    _process_failure_alert(
        run_id=run.run_id,
        job_name=run.job_name,
        tags=run.tags or {},
        error_message=error_message,
        log=context.log,
    )
