# =============================================================================
# Assets — Paquete ingestion (Bronze Layer)
# =============================================================================
#
# Este paquete contiene TODOS los assets de ingesta de datos (capa Bronze).
# Cada archivo representa una fuente de datos diferente:
#
#   tourism_arrivals.py   → Datos locales CSV de llegadas + ocupación
#   real_sources.py       → World Bank API + CITUR (datos.gov.co)
#   dane_sources.py       → DANE: PIB turístico de Colombia
#   migracion_sources.py  → Migración Colombia: flujos de viajeros
#
# Dagster carga automáticamente todos los assets exportados aquí.
# =============================================================================

# Fuentes originales: archivos CSV locales
from national_tourism.assets.ingestion.tourism_arrivals import *  # noqa: F401, F403

# Fuentes reales: APIs y datos abiertos
from national_tourism.assets.ingestion.real_sources import (  # noqa: F401
    raw_world_bank_arrivals,
    raw_citur_arrivals,
    raw_citur_hotel_occupancy,
    raw_world_bank_regional,
)

# NUEVAS FUENTES: DANE y Migración Colombia
from national_tourism.assets.ingestion.dane_sources import raw_dane_tourism_gdp  # noqa: F401
from national_tourism.assets.ingestion.migracion_sources import raw_migracion_flows  # noqa: F401

# NUEVAS FUENTES: Aerocivil y Banco de la República
from national_tourism.assets.ingestion.aerocivil_sources import raw_aerocivil_passengers  # noqa: F401
from national_tourism.assets.ingestion.banrep_sources import raw_banrep_tourism_balance  # noqa: F401
