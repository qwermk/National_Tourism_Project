# 📊 Análisis del Estado Actual del Proyecto

> **Fecha de análisis:** 11 de marzo de 2026
> **Rama:** `main`
> **Analizado por:** GitHub Copilot — revisión completa de todos los archivos fuente

---

## ✅ Completamente Implementado (listo para producción)

### Capa de Orquestación — Dagster

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `dagster/national_tourism/definitions.py` | ✅ Completo | Registra 7 assets (3 grupos Medallion), 2 resources, 1 schedule, 2 sensors |
| `dagster/national_tourism/assets/ingestion/tourism_arrivals.py` | ✅ Completo | 2 assets (`raw_tourism_arrivals`, `raw_hotel_occupancy`), ingesta CSV con fallback a datos sintéticos, upload a MinIO |
| `dagster/national_tourism/assets/staging/stg_tourism.py` | ✅ Completo | 2 assets con normalización real: dedup, type casting, manejo de nulos, estandarización de texto, clamping de ocupación |
| `dagster/national_tourism/assets/marts/tourism_marts.py` | ✅ Completo | 3 assets (`fct_tourism_arrivals`, `fct_hotel_occupancy`, `dim_departments`), escribe en MinIO Gold y DuckDB |
| `dagster/national_tourism/resources/duckdb_resource.py` | ✅ Completo | `ConfigurableResource` con integración S3/MinIO, creación de esquemas, import/export Parquet |
| `dagster/national_tourism/resources/minio_resource.py` | ✅ Completo | CRUD completo: upload, download, read Parquet, list objects, ensure bucket |
| `dagster/national_tourism/sensors/new_file_sensor.py` | ✅ Completo | Monitorea bucket `raw` de MinIO, cursor tracking de archivos procesados, `RunRequest` en nuevos archivos |
| `dagster/national_tourism/schedules/daily_schedule.py` | ✅ Completo | Cron diario 06:00 UTC, apunta a todos los assets, activación manual |
| `dagster/pyproject.toml` | ✅ Completo | Dependencias fijadas correctamente (dagster, dbt, duckdb, minio, pandas, pyarrow), ruff + pytest config |

### Capa de Transformación — dbt

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `dbt/dbt_project.yml` | ✅ Completo | Materialización Medallion correcta (views → ephemeral → tables), vars, schema routing |
| `dbt/profiles.yml` | ✅ Completo | Target DuckDB + settings MinIO S3. Target Snowflake comentado (Fase 2) |
| `dbt/packages.yml` | ✅ Completo | `dbt_utils` + `dbt_expectations` declarados |
| `dbt/models/staging/stg_tourism_arrivals.sql` | ✅ Completo | CTEs: type casts, `initcap`, `coalesce`, filtros de validez año/mes |
| `dbt/models/staging/stg_hotel_occupancy.sql` | ✅ Completo | `make_date`, clamping de ocupación, manejo de nulos |
| `dbt/models/intermediate/int_arrivals_monthly.sql` | ✅ Completo | Agregación mensual ephemeral que alimenta la tabla de hechos |
| `dbt/models/marts/fct_tourism_arrivals.sql` | ✅ Completo | Surrogate key vía `dbt_utils`, métricas finales, timestamp `loaded_at` |
| `dbt/models/marts/fct_hotel_occupancy.sql` | ✅ Completo | Surrogate key, ocupación calculada desde habitaciones, `loaded_at` |
| `dbt/models/marts/dim_departments.sql` | ✅ Completo | Passthrough directo del seed |
| `dbt/seeds/seed_departments.csv` | ✅ Completo | 33 departamentos colombianos con códigos DANE, capitales y regiones |
| `dbt/models/staging/_sources.yml` | ✅ Completo | Fuentes Bronze con `external_location` meta para integración DuckDB-Parquet |
| `dbt/models/staging/_staging_models.yml` | ✅ Completo | Tests `not_null`, `accepted_values`, rangos con `dbt_expectations` |
| `dbt/models/marts/_marts_models.yml` | ✅ Completo | Tests `unique`, `not_null`, rangos en surrogate keys y métricas clave |
| `dbt/tests/assert_valid_occupancy_rate.sql` | ✅ Completo | Test singular: consistencia entre ocupación reportada y calculada (umbral >10pp) |
| `dbt/tests/generic/assert_positive_values.sql` | ✅ Completo | Macro de test genérico reutilizable para validar columnas no negativas |

### Infraestructura — Docker

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `docker/docker-compose.yml` | ✅ Completo | MinIO + sidecar de init de buckets + Dagster webserver + Dagster daemon. Env vars, volumes, networks, healthchecks configurados |
| `docker/Dockerfile.dagster` | ✅ Completo | Python 3.11-slim, todas las dependencias pip, install editable del proyecto dagster, copia proyecto dbt |
| `docker/configs/dagster.yaml` | ✅ Completo | SQLite storage, `DagsterDaemonScheduler`, `DefaultRunLauncher`, logs locales, telemetry off |

### Dashboards — Evidence.dev

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `dashboards/evidence.config.yaml` | ✅ Completo | Apunta al archivo DuckDB |
| `dashboards/package.json` | ✅ Completo | Evidence core + conector DuckDB declarados |
| `dashboards/pages/index.md` | ✅ Completo | 4 queries SQL + gráficos: llegadas por año (barras), gasto (línea), top 10 países (tabla+barras), tendencia ocupación (línea), estacionalidad (barras) |
| `dashboards/pages/departamentos.md` | ✅ Completo | Filtro dropdown interactivo + gráfico de visitantes por departamento + gráfico de ocupación + tabla de datos |

### Scripts Auxiliares

| Archivo | Estado | Descripción |
|---------|--------|-------------|
| `scripts/download_sample_data.py` | ✅ Completo | API del Banco Mundial + generadores de datos sintéticos realistas (llegadas: ~12K+ registros con caída COVID, estacionalidad, distribución ponderada por país; ocupación: 12 departamentos × 6 años) |
| `scripts/setup_local.py` | ✅ Completo* | Crea directorios, copia `.env`, `pip install -e`, `dbt deps`, `dbt debug`. *Referencia `.env.example` que no existe aún |

---

## ⚠️ Parcialmente Implementado (estructura existe pero requiere trabajo)

| Item | Gap Específico | Impacto |
|------|---------------|---------|
| **Tests de Dagster** | Solo 2 smoke tests triviales. Sin tests de ejecución de assets, sin mocks de resources, sin tests de integración | Bajo hasta despliegue; Alto en producción |
| ~~**`dim_departments` en Dagster**~~ | ✅ **RESUELTO** — El asset Python (12 deptos) ya no se registra. `dbt build` carga `seed_departments.csv` → `gold.dim_departments` (33 deptos) | — |
| **Macro `generate_date_spine`** | Implementada y correcta pero **no la usa ningún modelo** — no existe tabla `dim_date` | Medio — análisis de series temporales incompleto |
| ~~**Integración Dagster ↔ dbt**~~ | ✅ **RESUELTO** — `DbtCliResource` + `@dbt_assets` + `NationalTourismDbtTranslator`. Pipeline unificado: ingestion Python → dbt build (staging + marts en DuckDB) | — |
| **Integración Evidence ↔ DuckDB** | Los dashboards consultan `gold.fct_tourism_arrivals` / `gold.fct_hotel_occupancy` que solo existen si el pipeline de Dagster (no dbt) corrió. No hay integración con el output de dbt | Alto — dashboards vacíos si solo se corre dbt |

---

## ❌ Completamente Faltante

| Item | Descripción | Impacto |
|------|-------------|---------|
| **`.env.example`** | Referenciado por `setup_local.py` como `infra/local/.env.example` pero no existe en el repositorio | Alto — setup para nuevos desarrolladores falla |
| ~~**Asset `@dbt_assets` en Dagster**~~ | ✅ **RESUELTO** — Implementado en `assets/dbt_assets.py` con `NationalTourismDbtTranslator`. Ver tarea 1. | — |
| **Modelo `dim_date`** | Macro `generate_date_spine` existe pero sin modelo que la use | Medio |
| **Evidence en Docker Compose** | No hay servicio para Evidence en `docker-compose.yml`. Debe correrse manualmente fuera del stack | Medio — onboarding frustrado |
| **CI/CD** | Sin GitHub Actions, sin Makefile, sin pre-commit hooks | Alto para colaboración |
| **Validación de datos en runtime** | Sin Great Expectations ni validación de calidad en el pipeline Dagster más allá de checks básicos de pandas | Medio |
| **Monitoring / alertas** | Sin alertas de Dagster en fallos, sin integración Slack/email | Bajo hasta producción |
| **Conectores a fuentes reales** | Integración con APIs de CITUR / Migración Colombia es aspiracional — solo existen rutas CSV/sintéticas | Alto para datos reales |
| **Fase 2 — Snowflake** | Target comentado en `profiles.yml`; `infra/snowflake/` es solo un README stub | Futuro |
| **Fase 3 — AWS** | `infra/aws/` es solo un README stub sin Terraform ni configuración | Futuro |
| **`data/seeds/`** | Directorio vacío y huérfano — los seeds de dbt viven en `dbt/seeds/`, no aquí | Bajo |
| **Tests unitarios completos** | Sin tests de resources, sensors, transformaciones ni casos edge | Alto para mantenibilidad |

---

## 🗺️ Hoja de Ruta — Próximos Pasos (por prioridad)

### 🔴 Prioridad 1 — Correcciones Críticas

```
[x] 1. Integrar Dagster ↔ dbt  ✅ COMPLETADO (09 mar 2026)
        - Creado DbtCliResource en resources/dbt_resource.py
        - Creado @dbt_assets en assets/dbt_assets.py con NationalTourismDbtTranslator
        - Translator mapea sources Bronze de dbt → asset keys de ingesta
        - Macro generate_schema_name.sql → marts usan schema "gold" (no "main_gold")
        - dbt_project.yml: +schema: marts → +schema: gold
        - definitions.py: pipeline unificado (ingestion Python + dbt build)
        - Staging y marts Python ya no se registran (dbt los reemplaza)

[x] 2. Crear infra/local/.env.example  ✅ YA EXISTÍA
        - Variables presentes: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
          DUCKDB_DATABASE, DAGSTER_HOME, DBT_PROFILES_DIR, DBT_TARGET
        - Consistent con docker-compose.yml

[x] 3. Sincronizar dim_departments  ✅ COMPLETADO (resuelto por tarea 1)
        - dbt dim_departments.sql lee de seed_departments.csv (33 departamentos)
        - El asset Python dim_departments (12 deptos hardcodeados) ya no se registra
        - Resultado: gold.dim_departments = 33 departamentos via dbt seed
```

### 🟡 Prioridad 2 — Completar el Stack

```
[x] 4. Crear modelo dim_date  ✅ COMPLETADO (10 mar 2026)
        - dbt/models/marts/dim_date.sql con generate_date_spine (2019–2024)
        - _marts_models.yml: tests unique+not_null en date_day, rangos año/mes/trimestre

[x] 5. Agregar Evidence al docker-compose.yml  ✅ COMPLETADO (10)
        - Servicio `evidence` (node:20-alpine): npm install && npm run dev
        - Puerto 3333, volume compartido dagster-data para leer tourism.duckdb
        - evidence.config.yaml: env var fallback ${EVIDENCE_DATABASE:../data/tourism.duckdb}
        - Resultado: `docker compose up` levanta el stack completo

[x] 6. Escribir tests reales para Dagster  ✅ COMPLETADO (10 mar 2026)
        - 35/35 tests pasando (test_definitions, test_ingestion, test_sensor, test_resources)
        - Mock de MinIO con MagicMock + ResourceDefinition.hardcoded_resource
        - Tests de lógica de transformación: columnas, rangos, nulls, uploads
        - Tests de cursor del sensor: first-run, nuevos archivos, sin cambios, errores
        - conftest.py regenera manifest.json automáticamente si no existe
```

### 🟢 Prioridad 3 — Resiliencia y Producción

```
[x] 7. Agregar CI/CD con GitHub Actions  ✅ COMPLETADO (11 mar 2026)
        - .github/workflows/ci.yml — 4 jobs encadenados:
          • lint: ruff check + ruff format --check en dagster/
          • dbt-test (needs: lint): deps → parse → compile → seed → schema tests
          • dagster-test (needs: lint): pytest -v --cov ≥ 60% + artefacto coverage.xml
          • docker-build (needs: dbt-test + dagster-test): build sin push, caché GHA
        - pyproject.toml: pytest-cov>=5.0 agregado a extras dev

[x] 8. Configurar alertas en Dagster  ✅ COMPLETADO (11 mar 2026)
        - dagster/national_tourism/sensors/alert_sensor.py — run_failure_sensor
          • DefaultSensorStatus.RUNNING (activo por defecto desde el primer `dagster dev`)
          • Extrae lógica core en _process_failure_alert() para testabilidad
        - Canal Slack: SLACK_WEBHOOK_URL (stdlib urllib, sin dependencias extra)
        - Canal Email: SMTP_HOST/PORT/USER/PASSWORD + ALERT_EMAIL_FROM/TO (stdlib smtplib + STARTTLS)
        - DAGSTER_UI_URL para incluir enlaces directos en los mensajes
        - Graceful no-op si ninguna variable está configurada (solo log)
        - Registrado en definitions.py junto a new_raw_file_sensor
        - infra/local/.env.example: documentadas todas las variables de alerta
        - dagster/tests/test_alert_sensor.py: 12/12 tests pasando
        - Suite completa: 47/47 tests pasando

[X] 9. Implementar conectores a fuentes reales  ✅ COMPLETADO
        - dagster/national_tourism/resources/http_resource.py
          • HttpResource (ConfigurableResource): requests session con retry
            exponencial (3 intentos, backoff 2×), métodos get_json(),
            get_text(), get_dataframe_from_csv(), get_dataframe_from_excel()
        - dagster/national_tourism/assets/ingestion/real_sources.py — 3 nuevos assets:
          • raw_world_bank_arrivals: World Bank Open Data API (ST.INT.ARVL,
            ST.INT.DPRT, ST.INT.RCPT.CD) → bronze/world_bank/arrivals_annual.parquet
          • raw_citur_arrivals: CITUR/datos.gov.co (var CITUR_ARRIVALS_URL)
            → bronze/citur/tourism_arrivals.parquet
          • raw_citur_hotel_occupancy: CITUR ocupación (CITUR_OCCUPANCY_URL)
            → bronze/citur/hotel_occupancy.parquet
        - Todos con fallback a datos sintéticos si fuente no disponible
        - Normalización robusta: 50+ variantes de nombres de columna soportadas
        - dbt: stg_world_bank_arrivals.sql + fuentes citur en _sources.yml
        - infra/local/.env.example: CITUR_ARRIVALS_URL, CITUR_OCCUPANCY_URL
        - dagster/tests/test_real_sources.py: 23/23 tests pasando
        - Suite completa: 70/70 tests pasando
```

### ⚪ Prioridad 4 — Escalabilidad (Fases 2 y 3)

```
[ ] 10. Configurar y probar target Snowflake
         - Descomentar profiles.yml Snowflake
         - Implementar infra/snowflake/ con Terraform o setup manual
         - Adaptar resources de Dagster para Snowflake

[ ] 11. Implementar infraestructura AWS
         - Terraform para S3, Redshift/Athena
         - Adaptar pipeline para S3 como fuente
         - Opcional: integración Databricks
```

---

## 📌 Resumen Ejecutivo

El proyecto tiene una **base sólida y bien estructurada**. Aproximadamente el **75% de la lógica core está implementada** con código real de calidad. Los gaps más urgentes son arquitectónicos (Dagster no orquesta dbt) más que faltantes de funcionalidad, lo que hace que el effort de completar Phase 1 sea manejable.

| Dimensión | Estado |
|-----------|--------|
| Dagster pipeline | 🟢 Funcional (Bronze → Silver → Gold) |
| dbt models | 🟢 Funcional (staging → marts con tests) |
| Docker infra | 🟡 Casi completo (falta Evidence) |
| Evidence dashboards | 🟢 Funcional (2 páginas interactivas) |
| Integración Dagster ↔ dbt | � Implementada (@dbt_assets + DbtCliResource) |
| Tests | 🟢 70/70 pasando (definitions, ingestion, sensor, resources, alert_sensor, real_sources) |
| CI/CD | 🟢 GitHub Actions: lint → dbt-test → dagster-test → docker-build |
| Alertas | 🟢 run_failure_sensor: Slack + email (stdlib, sin deps extra) |
| Datos reales | 🟢 HttpResource + World Bank API + CITUR (fallback sintético) |
| Documentación | 🟢 README completo (EN + ES) |
