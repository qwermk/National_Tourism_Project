# =============================================================================
# Script: setup_local.py — Configuración inicial del entorno local
# =============================================================================
# Ejecutar una vez después de clonar el repositorio:
#   python scripts/setup_local.py
# =============================================================================

import os
import shutil
import subprocess
import sys
from pathlib import Path


def main():
    """Configura el entorno local del proyecto."""
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    print("=" * 60)
    print("🏗️  National Tourism Project — Setup Local")
    print("=" * 60)

    # 1. Crear directorios necesarios
    print("\n📁 Creando directorios...")
    dirs_to_create = [
        "data/raw",
        "data/seeds",
        "dagster/.dagster_home",
    ]
    for d in dirs_to_create:
        Path(d).mkdir(parents=True, exist_ok=True)
        print(f"   ✓ {d}")

    # 2. Crear .gitkeep en directorios vacíos
    for d in dirs_to_create:
        gitkeep = Path(d) / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()

    # 3. Copiar .env si no existe
    env_file = project_root / ".env"
    env_example = project_root / "infra" / "local" / ".env.example"
    if not env_file.exists() and env_example.exists():
        shutil.copy(env_example, env_file)
        print("\n📋 .env copiado desde infra/local/.env.example")
    else:
        print("\n📋 .env ya existe o no se encontró el ejemplo")

    # 4. Instalar dependencias de Python
    print("\n🐍 Instalando dependencias Python...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", "dagster/.[dev]"],
        check=True,
    )

    # 5. Instalar paquetes dbt
    print("\n📦 Instalando paquetes dbt...")
    subprocess.run(
        ["dbt", "deps", "--profiles-dir", "dbt"],
        cwd=project_root / "dbt",
        check=True,
    )

    # 6. Verificar dbt
    print("\n🔍 Verificando conexión dbt...")
    subprocess.run(
        ["dbt", "debug", "--profiles-dir", "dbt", "--target", "local"],
        cwd=project_root / "dbt",
        check=False,  # No fallar si DuckDB no está listo aún
    )

    print("\n" + "=" * 60)
    print("✅ Setup completado!")
    print("=" * 60)
    print("""
Próximos pasos:
  1. Levantar servicios Docker:
     docker compose -f docker/docker-compose.yml up -d

  2. Abrir Dagster UI:
     http://localhost:3000

  3. Materializar los assets desde la UI de Dagster

  4. Explorar dashboards Evidence.dev:
     cd dashboards && npm install && npm run dev
""")


if __name__ == "__main__":
    main()
