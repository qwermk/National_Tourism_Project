# =============================================================================
# conftest.py — Fixtures de sesión para el conjunto de tests
# =============================================================================
# Garantiza que el manifest.json de dbt exista antes de ejecutar cualquier
# test que importe national_tourism.definitions (que carga dbt_assets).
# =============================================================================

import subprocess
import sys
from pathlib import Path

import pytest

DBT_DIR = Path(__file__).parent.parent.parent / "dbt"
MANIFEST_PATH = DBT_DIR / "target" / "manifest.json"


@pytest.fixture(scope="session", autouse=True)
def ensure_dbt_manifest():
    """Genera dbt/target/manifest.json si no existe o está desactualizado."""
    if not MANIFEST_PATH.exists():
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dbt",
                "parse",
                "--profiles-dir",
                str(DBT_DIR),
                "--project-dir",
                str(DBT_DIR),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(
                f"dbt parse falló al generar manifest.json:\n{result.stderr}"
            )
