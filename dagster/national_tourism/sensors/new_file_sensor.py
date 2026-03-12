# =============================================================================
# Sensor — Detecta nuevos archivos en MinIO bucket 'raw'
# =============================================================================
# Este sensor monitorea el bucket 'raw' de MinIO y dispara una
# materialización cuando detecta nuevos archivos CSV/Parquet.
# =============================================================================

from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    sensor,
)


@sensor(
    name="new_raw_file_sensor",
    description="Detecta nuevos archivos en MinIO bucket 'raw' y dispara el pipeline.",
    target=AssetSelection.assets("raw_tourism_arrivals", "raw_hotel_occupancy"),
    minimum_interval_seconds=60,  # Verificar cada minuto
    default_status=DefaultSensorStatus.STOPPED,
    required_resource_keys={"minio"},
)
def new_raw_file_sensor(context: SensorEvaluationContext, minio):
    """
    Monitorea MinIO por nuevos archivos y dispara materialización.

    Usa el cursor para trackear qué archivos ya fueron procesados.
    """
    # Listar archivos actuales en el bucket raw
    try:
        current_files = set(minio.list_objects("raw"))
    except Exception as e:
        context.log.warning(f"No se pudo conectar a MinIO: {e}")
        return

    # Obtener archivos ya procesados del cursor
    previous_files = set()
    if context.cursor:
        previous_files = set(context.cursor.split(","))

    # Detectar archivos nuevos
    new_files = current_files - previous_files

    if new_files:
        context.log.info(f"Nuevos archivos detectados: {new_files}")

        # Actualizar cursor con todos los archivos conocidos
        all_files = current_files | previous_files
        context.update_cursor(",".join(sorted(all_files)))

        yield RunRequest(
            run_key=f"new_files_{'_'.join(sorted(new_files))}",
            tags={"triggered_by": "new_raw_file_sensor"},
        )
    else:
        context.log.debug("No se detectaron archivos nuevos.")
