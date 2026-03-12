# =============================================================================
# dbt Resource — Integración Dagster ↔ dbt via CLI
# =============================================================================
# Configura el DbtCliResource apuntando al proyecto dbt local.
# Este resource es inyectado en el @dbt_assets para ejecutar comandos dbt.
# =============================================================================

import shutil
import sys
from pathlib import Path

from dagster_dbt import DbtCliResource

# Ruta al directorio del proyecto dbt (relativa a este archivo)
# dagster/national_tourism/resources/ → project_root/dbt/
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"


def _find_dbt_executable() -> str:
    """Locate the dbt CLI in the active Python environment or PATH."""
    scripts_dir = Path(sys.executable).parent
    for name in ("dbt.exe", "dbt"):
        candidate = scripts_dir / name
        if candidate.exists():
            return str(candidate)
    found = shutil.which("dbt")
    return found if found else "dbt"


dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROJECT_DIR),
    target="local",
    dbt_executable=_find_dbt_executable(),
)
