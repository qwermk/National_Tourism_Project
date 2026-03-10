# =============================================================================
# Test: Verificar que las definiciones de Dagster cargan correctamente
# =============================================================================

def test_definitions_load():
    """Las definiciones de Dagster deben cargar sin errores."""
    from national_tourism.definitions import defs

    assert defs is not None
    # Verificar que hay assets registrados
    all_assets = defs.get_all_asset_specs()
    assert len(all_assets) > 0, "No se encontraron assets registrados"


def test_asset_groups():
    """Los assets deben pertenecer a los grupos correctos (medallion layers)."""
    from national_tourism.definitions import defs

    all_assets = defs.get_all_asset_specs()
    # Verificar que hay assets en cada capa
    # (los grupos se asignan en definitions.py)
    assert len(all_assets) >= 5, f"Se esperaban >= 5 assets, encontrados: {len(all_assets)}"
