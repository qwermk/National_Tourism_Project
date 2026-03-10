# =============================================================================
# Configuración Local — README
# =============================================================================

# Fase 1: Modern Data Stack Local (100% Open Source)

> Todas las herramientas usadas en esta fase son open-source y gratuitas.
> Se usa **Dagster OSS** (Apache 2.0), no Dagster Cloud.

## Componentes

| Servicio | Puerto | URL | Licencia |
|----------|--------|-----|----------|
| MinIO API | 9000 | http://localhost:9000 | AGPL v3 |
| MinIO Console | 9001 | http://localhost:9001 | AGPL v3 |
| Dagster UI (OSS) | 3000 | http://localhost:3000 | Apache 2.0 |
| Evidence.dev | 3333 | http://localhost:3333 | MIT |

## Setup

```bash
# 1. Copiar variables de entorno
cp infra/local/.env.example .env

# 2. Levantar servicios Docker
docker compose -f docker/docker-compose.yml up -d

# 3. Instalar dependencias Python
pip install -e "dagster/.[dev]"

# 4. Instalar paquetes dbt
cd dbt && dbt deps && cd ..

# 5. Verificar conexión
dbt debug --profiles-dir dbt --target local
```

## Arquitectura

```
CSV/API → Dagster (ingest) → MinIO (Bronze) → dbt + DuckDB (Silver/Gold) → Evidence.dev
```
