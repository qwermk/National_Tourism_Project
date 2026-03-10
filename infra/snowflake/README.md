# Fase 2: Snowflake (pendiente)

Este directorio contendrá la configuración para la versión Snowflake del pipeline.

## Cambios respecto a la versión local

| Componente | Local | Snowflake |
|------------|-------|-----------|
| Almacenamiento | MinIO | Snowflake Stages / S3 |
| Motor SQL | DuckDB | Snowflake |
| dbt adapter | dbt-duckdb | dbt-snowflake |
| Orquestador | Dagster | Dagster |

## TODO
- [ ] Crear cuenta de Snowflake
- [ ] Configurar warehouse y database
- [ ] Adaptar dbt profiles
- [ ] Crear Dagster resources para Snowflake
- [ ] Migrar assets de ingesta
