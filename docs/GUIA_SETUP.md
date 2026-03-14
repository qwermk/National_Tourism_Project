# 📘 Guía de Configuración — Paso a Paso

**Nivel:** Principiante · Junior Data Engineer  
**Idioma:** Español  

---

## Índice

1. [¿Qué es este proyecto?](#1-qué-es-este-proyecto)
2. [Prerrequisitos](#2-prerrequisitos)
3. [Instalación paso a paso](#3-instalación-paso-a-paso)
4. [Ejecutar el pipeline completo](#4-ejecutar-el-pipeline-completo)
5. [Acceder a los dashboards](#5-acceder-a-los-dashboards)
6. [Arquitectura explicada](#6-arquitectura-explicada)
7. [Glosario de conceptos clave](#7-glosario-de-conceptos-clave)
8. [Estructura de archivos](#8-estructura-de-archivos)
9. [Solución de problemas comunes](#9-solución-de-problemas-comunes)

---

## 1. ¿Qué es este proyecto?

Es un pipeline de datos de extremo a extremo que analiza el turismo en Colombia.
Los datos pasan por 3 capas (la **Arquitectura Medallion**):

| Capa | Nombre | ¿Qué contiene? |
|------|--------|-----------------|
| 🥉 | **Bronze** | Datos crudos tal como los entrega la fuente (sin modificar) |
| 🥈 | **Silver** | Datos limpiados: columnas renombradas, tipos corregidos, duplicados eliminados |
| 🥇 | **Gold** | Modelos listos para análisis: tablas de hechos y dimensiones |

### Flujo resumido

```
Fuentes externas (CITUR, DANE, Migración, World Bank)
       ↓   (Python descarga e ingesta)
    MinIO  ← archivos Parquet en bucket "bronze"
       ↓   (dbt transforma con SQL)
   DuckDB  ← staging views (Silver) → fact tables (Gold)
       ↓
  Streamlit ← dashboard interactivo con gráficas
```

### Herramientas que usa

| Herramienta | ¿Para qué sirve? | ¿Dónde se configura? |
|-------------|-------------------|----------------------|
| **Dagster** | Orquestar (decidir qué se ejecuta, en qué orden, y cuándo) | `dagster/` |
| **MinIO** | Almacenar archivos Parquet (como un "mini AWS S3" local) | Docker — puerto 9001 |
| **DuckDB** | Base de datos analítica (muy rápida para consultas) | `data/tourism.duckdb` |
| **dbt** | Transformar datos con SQL (Bronze → Silver → Gold) | `dbt/` |
| **Streamlit** | Mostrar dashboards interactivos con gráficas | `dashboards/app.py` |
| **Docker** | Levantar todo con un solo comando | `docker/docker-compose.yml` |

---

## 2. Prerrequisitos

Antes de empezar, necesitas tener instalado:

| Herramienta | Versión mínima | ¿Cómo verificar? |
|-------------|---------------|-------------------|
| Git | cualquiera | `git --version` |
| Python | 3.11+ | `python --version` |
| Docker | 20.10+ | `docker --version` |
| Docker Compose | v2+ | `docker compose version` |

> **¿No tienes Python?** Descárgalo de https://www.python.org/downloads/  
> **¿No tienes Docker?** Descarga Docker Desktop: https://www.docker.com/products/docker-desktop

---

## 3. Instalación paso a paso

### 3.1 Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/National_Tourism_Project.git
cd National_Tourism_Project
```

### 3.2 Crear un entorno virtual de Python

```bash
# Crear el entorno virtual
python -m venv .venv

# Activar (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Activar (Linux / macOS)
source .venv/bin/activate
```

> **¿Qué es un entorno virtual?** Es una carpeta aislada con las dependencias del
> proyecto. Así no mezclas paquetes con otros proyectos de tu computadora.

### 3.3 Instalar dependencias de Python

```bash
# Instalar Dagster y todas las dependencias del proyecto
pip install -e "dagster/.[dev]"
```

Esto instala: Dagster, dbt, MinIO client, DuckDB, Streamlit, Plotly, y más.

### 3.4 Instalar paquetes de dbt

```bash
cd dbt
dbt deps
cd ..
```

> **¿Qué hace `dbt deps`?** Descarga los paquetes de dbt que el proyecto necesita
> (`dbt_utils`, `dbt_expectations`, `dbt_date`). Son como "librerías" de SQL reutilizable.

### 3.5 Levantar los servicios con Docker

```bash
docker compose -f docker/docker-compose.yml up -d
```

Esto levanta 4 servicios:

| Servicio | Puerto | URL |
|----------|--------|-----|
| MinIO (API S3) | 9000 | — |
| MinIO (Consola web) | 9001 | http://localhost:9001 |
| Dagster (UI) | 3000 | http://localhost:3000 |
| Streamlit (Dashboard) | 8501 | http://localhost:8501 |

> **Credenciales de MinIO:** usuario `minioadmin`, contraseña `minioadmin`

### 3.6 Verificar que todo está corriendo

```bash
docker compose -f docker/docker-compose.yml ps
```

Deberías ver los 4 servicios con estado "running" o "Up".

---

## 4. Ejecutar el pipeline completo

### Opción A — Desde la interfaz de Dagster (recomendado)

1. Abre http://localhost:3000 en tu navegador
2. Ve a la sección **Assets** en el menú lateral
3. Haz clic en **Materialize all** para ejecutar todos los assets
4. Espera a que terminen (los assets se pondrán verdes)

### Opción B — Desde la línea de comandos

```bash
# Asegúrate de estar en la carpeta dagster/
cd dagster

# Materializar todos los assets de ingesta
dagster asset materialize --select "raw_tourism_arrivals raw_hotel_occupancy raw_world_bank_arrivals raw_citur_arrivals raw_citur_hotel_occupancy raw_dane_tourism_gdp raw_migracion_flows"

# Ejecutar dbt para las transformaciones
cd ../dbt
dbt run
dbt test  # Ejecutar tests de calidad de datos
```

### ¿Qué pasa al ejecutar el pipeline?

1. **Ingesta (Python):** Se descargan datos de 5 fuentes y se guardan como Parquet en MinIO
2. **Transformación (dbt):** Se ejecutan los modelos SQL que limpian y agregan los datos
3. **Resultado:** DuckDB queda poblado con las tablas Gold, listas para el dashboard

### Verificar los datos

```bash
cd dbt
dbt run       # Crea las tablas/vistas en DuckDB
dbt test      # Ejecuta tests de calidad
dbt docs generate && dbt docs serve  # Genera documentación visual
```

---

## 5. Acceder a los dashboards

### Streamlit (Dashboard principal)

- **URL:** http://localhost:8501
- **¿Qué muestra?** 4 pestañas interactivas:
  1. **Llegadas de Turistas** — Gráficas de barras y líneas con llegadas por país y departamento
  2. **Ocupación Hotelera** — Tendencia de ocupación por departamento
  3. **PIB Turístico** — Indicadores económicos del turismo (fuente: DANE)
  4. **Flujos Migratorios** — Entradas y salidas de viajeros (fuente: Migración Colombia)

- **Filtros:** En la barra lateral puedes filtrar por rango de años y departamento

### MinIO (Explorar archivos Parquet)

- **URL:** http://localhost:9001
- **Usuario:** minioadmin / **Contraseña:** minioadmin
- **¿Qué ver?** Los buckets `bronze`, `silver`, `gold` con los archivos Parquet

### Dagster (Monitorear ejecuciones)

- **URL:** http://localhost:3000
- **¿Qué ver?** El grafo de assets, historial de ejecuciones, logs, y estado del pipeline

---

## 6. Arquitectura explicada

### Capa Bronze (Ingesta — Python)

Los archivos de Python en `dagster/national_tourism/assets/ingestion/` descargan datos
de fuentes externas y los guardan como Parquet en MinIO.

| Asset | Fuente | ¿Qué descarga? |
|-------|--------|-----------------|
| `raw_tourism_arrivals` | CSV local / CITUR | Llegadas de turistas por departamento |
| `raw_hotel_occupancy` | CSV local / CITUR | Ocupación hotelera mensual |
| `raw_world_bank_arrivals` | World Bank API | Indicadores internacionales de turismo |
| `raw_citur_arrivals` | CITUR API | Llegadas detalladas con país de origen |
| `raw_citur_hotel_occupancy` | CITUR API | Datos detallados de ocupación hotelera |
| `raw_dane_tourism_gdp` | DANE API | PIB turístico, empleo, crecimiento |
| `raw_migracion_flows` | Migración Col. API | Entradas/salidas por nacionalidad |

> **Importante:** Si las APIs externas no están disponibles, los assets generan
> datos sintéticos de respaldo. Así siempre puedes trabajar con el pipeline.

### Capa Silver (Limpieza — dbt SQL)

Los modelos SQL en `dbt/models/staging/` limpian los datos Bronze:
- Renombran columnas a nombres estándar en español
- Convierten tipos de datos (string → integer, string → date)
- Eliminan nulos y duplicados
- Filtran registros inválidos

| Modelo dbt | ¿Qué limpia? |
|------------|---------------|
| `stg_tourism_arrivals` | Llegadas de turistas |
| `stg_hotel_occupancy` | Ocupación hotelera |
| `stg_world_bank_arrivals` | Indicadores del Banco Mundial |
| `stg_dane_tourism_gdp` | PIB turístico del DANE |
| `stg_migracion_flows` | Flujos migratorios |

### Capa Gold (Modelos de negocio — dbt SQL)

Los modelos SQL en `dbt/models/marts/` crean tablas listas para análisis:

| Modelo dbt | Tipo | ¿Qué contiene? |
|------------|------|-----------------|
| `fct_tourism_arrivals` | Hecho | Llegadas de turistas con métricas agregadas |
| `fct_hotel_occupancy` | Hecho | Indicadores de ocupación hotelera |
| `fct_tourism_gdp` | Hecho | PIB turístico anual |
| `fct_migration_flows` | Hecho | Entradas/salidas de viajeros |
| `dim_departments` | Dimensión | Catálogo de departamentos de Colombia |
| `dim_date` | Dimensión | Calendario con atributos temporales |

---

## 7. Glosario de conceptos clave

| Concepto | ¿Qué es? |
|----------|-----------|
| **Asset** | En Dagster, un asset es algo que tu pipeline produce (un archivo, una tabla, un modelo). Es la unidad básica de trabajo. |
| **Materializar** | Ejecutar un asset para que genere su resultado. Similar a "correr" un script. |
| **Resource** | Una conexión a un servicio externo (MinIO, DuckDB, etc.). Se configura una vez y la usan todos los assets. |
| **Sensor** | Un disparador automático. El sensor de MinIO detecta archivos nuevos y ejecuta el pipeline. |
| **Schedule** | Una programación temporal. El proyecto tiene un schedule diario a las 6:00 UTC. |
| **Parquet** | Formato de archivo columnar muy eficiente para datos analíticos. Más rápido que CSV. |
| **dbt** | "Data Build Tool". Permite transformar datos usando solo SQL, con modelos bien organizados. |
| **Medallion** | Arquitectura de datos en 3 capas: Bronze (crudo), Silver (limpio), Gold (listo para negocio). |
| **DuckDB** | Motor de base de datos analítica que funciona dentro de tu proceso (sin servidor separado). |
| **MinIO** | Almacenamiento de objetos compatible con Amazon S3. Piensa en él como un "S3 local". |
| **Fact table** | Tabla con eventos medibles (llegadas, ventas). Tiene métricas numéricas. |
| **Dimension table** | Tabla con atributos descriptivos (departamentos, fechas). Se usa para filtrar y agrupar facts. |
| **Surrogate key** | Clave primaria artificial generada a partir de columnas existentes (hash MD5). |

---

## 8. Estructura de archivos

```
National_Tourism_Project/
│
├── dagster/                              # 🎯 Orquestación
│   ├── pyproject.toml                    # Dependencias del proyecto
│   └── national_tourism/
│       ├── definitions.py                # Punto de entrada — registra TODO
│       ├── assets/
│       │   ├── ingestion/                # Bronze: descarga de fuentes
│       │   │   ├── tourism_arrivals.py   # CITUR: llegadas + ocupación
│       │   │   ├── real_sources.py       # World Bank + CITUR APIs
│       │   │   ├── dane_sources.py       # DANE: PIB turístico
│       │   │   └── migracion_sources.py  # Migración Colombia: flujos
│       │   ├── staging/                  # (Legacy — reemplazado por dbt)
│       │   ├── marts/                    # (Legacy — reemplazado por dbt)
│       │   └── dbt_assets.py             # Ejecuta dbt desde Dagster
│       ├── resources/                    # Conexiones (MinIO, DuckDB, HTTP)
│       ├── sensors/                      # Detectar archivos + alertas
│       └── schedules/                    # Ejecución diaria 6:00 UTC
│
├── dbt/                                  # 🔄 Transformaciones SQL
│   ├── dbt_project.yml                   # Configuración del proyecto dbt
│   ├── profiles.yml                      # Conexión a DuckDB
│   ├── models/
│   │   ├── staging/                      # Silver: limpieza de datos
│   │   │   ├── stg_tourism_arrivals.sql
│   │   │   ├── stg_hotel_occupancy.sql
│   │   │   ├── stg_world_bank_arrivals.sql
│   │   │   ├── stg_dane_tourism_gdp.sql
│   │   │   ├── stg_migracion_flows.sql
│   │   │   ├── _sources.yml             # Definición de fuentes
│   │   │   └── _staging_models.yml       # Tests de staging
│   │   ├── intermediate/                 # Cálculos intermedios
│   │   └── marts/                        # Gold: tablas de negocio
│   │       ├── fct_tourism_arrivals.sql
│   │       ├── fct_hotel_occupancy.sql
│   │       ├── fct_tourism_gdp.sql
│   │       ├── fct_migration_flows.sql
│   │       ├── dim_departments.sql
│   │       ├── dim_date.sql
│   │       └── _marts_models.yml         # Tests de marts
│   ├── seeds/                            # Catálogos (CSV)
│   ├── tests/                            # Tests de calidad
│   └── macros/                           # SQL reutilizable
│
├── dashboards/                           # 📊 Visualización
│   ├── app.py                            # Dashboard Streamlit (principal)
│   └── requirements.txt                  # Dependencias del dashboard
│
├── docker/                               # 🐳 Infraestructura
│   ├── docker-compose.yml                # Levanta MinIO + Dagster + Streamlit
│   ├── Dockerfile.dagster                # Imagen Docker para Dagster
│   └── configs/                          # Configuraciones de Dagster
│
├── data/                                 # 💾 Datos locales
│   ├── tourism.duckdb                    # Base de datos DuckDB (generada)
│   ├── raw/                              # Archivos CSV descargados
│   └── seeds/                            # Datos semilla
│
├── scripts/                              # 🛠️ Scripts auxiliares
│   ├── setup_local.py                    # Configuración inicial
│   ├── download_sample_data.py           # Descargar datos de ejemplo
│   └── dagster_api.py                    # Cliente API de Dagster
│
├── infra/                                # ☁️ Config por plataforma
│   ├── local/                            # Fase 1: Local
│   ├── snowflake/                        # Fase 2: Snowflake
│   └── aws/                              # Fase 3: AWS
│
├── README.md                             # Documentación (inglés)
├── README.es.md                          # Documentación (español)
└── docs/
    └── GUIA_SETUP.md                     # Esta guía
```

---

## 9. Solución de problemas comunes

### "Docker no puede levantar los servicios"

```bash
# Verificar que Docker está corriendo
docker info

# Reiniciar los servicios
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d

# Ver logs de un servicio específico
docker compose -f docker/docker-compose.yml logs dagster-webserver
```

### "dbt run falla con error de conexión"

```bash
# Verificar que el perfil de dbt apunta al archivo correcto
cat dbt/profiles.yml  # debe tener path: ../data/tourism.duckdb

# Verificar que el archivo DuckDB existe
ls data/tourism.duckdb
```

### "El dashboard Streamlit muestra 'No se encontraron tablas'"

Esto pasa cuando DuckDB no tiene datos todavía. Solución:
1. Ejecuta el pipeline de ingesta primero (desde Dagster UI o CLI)
2. Luego ejecuta `dbt run` para crear las tablas Gold
3. Recarga el dashboard en el navegador

### "MinIO muestra error de conexión"

```bash
# Verificar el estado del servicio
docker compose -f docker/docker-compose.yml ps minio

# Las credenciales por defecto son:
# - Endpoint: localhost:9000
# - Access Key: minioadmin
# - Secret Key: minioadmin
```

### "Los assets generan datos sintéticos en vez de datos reales"

Es normal. Las APIs de CITUR, DANE y Migración Colombia pueden no estar
disponibles todo el tiempo. Los datos sintéticos permiten:
- Trabajar con el pipeline sin depender de fuentes externas
- Probar las transformaciones dbt y los dashboards
- Aprender la arquitectura sin necesitar acceso a internet

Para usar datos reales, configura las variables de entorno:
```bash
export DANE_TOURISM_GDP_URL="https://www.datos.gov.co/..."
export MIGRACION_FLOWS_URL="https://www.datos.gov.co/..."
```

---

**¿Tienes más preguntas?** Abre un [Issue](../../issues) en el repositorio.
