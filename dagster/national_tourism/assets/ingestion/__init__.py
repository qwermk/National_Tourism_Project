# =============================================================================
# Assets — Paquete ingestion (Bronze Layer)
# =============================================================================
from national_tourism.assets.ingestion.tourism_arrivals import *  # noqa: F401, F403
from national_tourism.assets.ingestion.real_sources import (  # noqa: F401
    raw_world_bank_arrivals,
    raw_citur_arrivals,
    raw_citur_hotel_occupancy,
)
