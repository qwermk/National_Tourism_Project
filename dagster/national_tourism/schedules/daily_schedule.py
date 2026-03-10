# =============================================================================
# Schedule — Ejecución diaria del pipeline de turismo
# =============================================================================

from dagster import (
    AssetSelection,
    ScheduleDefinition,
)

# ---------------------------------------------------------------------------
# Schedule diario: materializa todos los assets a las 6:00 AM UTC
# ---------------------------------------------------------------------------
daily_tourism_schedule = ScheduleDefinition(
    name="daily_tourism_pipeline",
    target=AssetSelection.all(),
    cron_schedule="0 6 * * *",  # Todos los días a las 6:00 AM UTC
    description="Ejecuta el pipeline completo de turismo diariamente a las 6:00 AM UTC.",
    default_status=None,  # Se activa manualmente desde la UI
)
