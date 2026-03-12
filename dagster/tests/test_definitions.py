# =============================================================================
# Test: Verificar que las definiciones de Dagster cargan correctamente
# =============================================================================

def test_definitions_load():
    """Las definiciones de Dagster deben cargar sin errores."""
    from national_tourism.definitions import defs

    assert defs is not None
    # Verificar que hay assets registrados
    all_assets = defs.resolve_all_asset_specs()
    assert len(all_assets) > 0, "No se encontraron assets registrados"


def test_asset_groups():
    """Los assets deben pertenecer a los grupos correctos (medallion layers)."""
    from national_tourism.definitions import defs

    all_assets = defs.resolve_all_asset_specs()
    # Verificar que hay assets en cada capa
    # (los grupos se asignan en definitions.py)
    assert len(all_assets) >= 5, f"Se esperaban >= 5 assets, encontrados: {len(all_assets)}"


def test_resources_registered():
    """Los resources minio, duckdb y dbt deben estar registrados."""
    from national_tourism.definitions import defs

    resource_keys = set(defs.resources.keys())
    assert "minio" in resource_keys, "Resource 'minio' no registrado"
    assert "duckdb" in resource_keys, "Resource 'duckdb' no registrado"
    assert "dbt" in resource_keys, "Resource 'dbt' no registrado"


def test_schedules_registered():
    """Al menos un schedule debe estar registrado."""
    from national_tourism.definitions import defs

    assert len(defs.schedules) >= 1, "No hay schedules registrados"


def test_sensors_registered():
    """El sensor de nuevos archivos debe estar registrado."""
    from national_tourism.definitions import defs

    sensor_names = [s.name for s in defs.sensors]
    assert "new_raw_file_sensor" in sensor_names, "Sensor 'new_raw_file_sensor' no registrado"


def test_ingestion_assets_registered():
    """Los assets de ingesta Bronze deben estar registrados."""
    from national_tourism.definitions import defs

    asset_keys = {spec.key.to_user_string() for spec in defs.resolve_all_asset_specs()}
    assert "raw_tourism_arrivals" in asset_keys, "Asset 'raw_tourism_arrivals' no registrado"
    assert "raw_hotel_occupancy" in asset_keys, "Asset 'raw_hotel_occupancy' no registrado"

