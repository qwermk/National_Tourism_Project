# =============================================================================
# Script: download_sample_data.py — Descarga datos de ejemplo
# =============================================================================
# Descarga datos públicos de turismo colombiano desde fuentes abiertas.
# Ejecutar: python scripts/download_sample_data.py
# =============================================================================

import os
from pathlib import Path

import pandas as pd
import requests


def main():
    """Descarga datos de ejemplo para el proyecto."""
    project_root = Path(__file__).parent.parent
    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("📥 Descargando datos de turismo de Colombia")
    print("=" * 60)

    # -----------------------------------------------------------------
    # Fuente 1: World Bank — International Tourism Arrivals
    # -----------------------------------------------------------------
    print("\n1. World Bank — Turismo internacional en Colombia...")
    try:
        # API del World Bank: International tourism, number of arrivals
        url = (
            "https://api.worldbank.org/v2/country/COL/indicator/"
            "ST.INT.ARVL?format=json&per_page=50&date=2010:2024"
        )
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        if len(data) > 1 and data[1]:
            records = [
                {
                    "anio": item["date"],
                    "pais": "Colombia",
                    "indicador": item["indicator"]["value"],
                    "valor": item["value"],
                }
                for item in data[1]
                if item["value"] is not None
            ]
            df = pd.DataFrame(records)
            output_path = raw_dir / "world_bank_tourism_arrivals.csv"
            df.to_csv(output_path, index=False)
            print(f"   ✓ Guardado: {output_path} ({len(df)} registros)")
        else:
            print("   ⚠ No se encontraron datos en la API del World Bank")
    except Exception as e:
        print(f"   ✗ Error descargando del World Bank: {e}")

    # -----------------------------------------------------------------
    # Fuente 2: Generar datos sintéticos realistas
    # -----------------------------------------------------------------
    print("\n2. Generando datos sintéticos realistas...")
    try:
        _generate_synthetic_arrivals(raw_dir)
        _generate_synthetic_occupancy(raw_dir)
        print("   ✓ Datos sintéticos generados correctamente")
    except Exception as e:
        print(f"   ✗ Error generando datos sintéticos: {e}")

    print("\n" + "=" * 60)
    print("✅ Descarga completada!")
    print(f"📁 Archivos guardados en: {raw_dir}")
    print("=" * 60)


def _generate_synthetic_arrivals(output_dir: Path):
    """Genera datos sintéticos de llegadas de turistas."""
    import numpy as np

    np.random.seed(42)

    countries = {
        "Estados Unidos": 0.22, "Brasil": 0.08, "México": 0.07,
        "Argentina": 0.06, "Ecuador": 0.09, "Perú": 0.06,
        "Chile": 0.05, "España": 0.04, "Alemania": 0.03,
        "Francia": 0.03, "Reino Unido": 0.03, "Canadá": 0.04,
        "Panamá": 0.05, "Venezuela": 0.08, "Costa Rica": 0.03,
        "Italia": 0.02, "Países Bajos": 0.02,
    }

    departments = [
        "Bogotá D.C.", "Antioquia", "Bolívar", "Valle Del Cauca",
        "Atlántico", "Santander", "San Andrés", "Magdalena",
        "Nariño", "Risaralda", "Quindío", "Boyacá",
    ]

    records = []
    for year in range(2019, 2025):
        for month in range(1, 13):
            # Base mensual con estacionalidad
            base = 35000
            if month in [1, 7, 12]:
                base *= 1.4
            elif month in [6, 8]:
                base *= 1.2
            elif month in [2, 9]:
                base *= 0.85

            # Efecto COVID
            if year == 2020:
                if month >= 3:
                    base *= 0.15
            elif year == 2021 and month <= 6:
                base *= 0.5

            # Crecimiento anual post-COVID
            if year >= 2022:
                base *= 1 + (year - 2021) * 0.12

            for country, share in countries.items():
                for dept in np.random.choice(departments, size=3, replace=False):
                    visitors = int(base * share * np.random.uniform(0.02, 0.15))
                    if visitors > 0:
                        records.append({
                            "fecha_llegada": f"{year}-{month:02d}-15",
                            "anio": year,
                            "mes": month,
                            "pais_origen": country,
                            "departamento_destino": dept,
                            "motivo_viaje": np.random.choice(
                                ["Turismo", "Negocios", "Eventos", "Educación", "Salud"],
                                p=[0.55, 0.20, 0.10, 0.08, 0.07],
                            ),
                            "punto_entrada": np.random.choice(
                                ["Aéreo", "Terrestre", "Marítimo"],
                                p=[0.70, 0.22, 0.08],
                            ),
                            "numero_visitantes": visitors,
                            "gasto_estimado_usd": round(
                                visitors * np.random.uniform(800, 2500), 2
                            ),
                        })

    df = pd.DataFrame(records)
    output_path = output_dir / "llegadas_turistas_sintetico.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"   ✓ Llegadas: {output_path} ({len(df)} registros)")


def _generate_synthetic_occupancy(output_dir: Path):
    """Genera datos sintéticos de ocupación hotelera."""
    import numpy as np

    np.random.seed(456)

    departments = [
        "Bogotá D.C.", "Antioquia", "Bolívar", "Valle Del Cauca",
        "Atlántico", "Santander", "San Andrés", "Magdalena",
        "Nariño", "Risaralda", "Quindío", "Boyacá",
    ]

    records = []
    for year in range(2019, 2025):
        for month in range(1, 13):
            for dept in departments:
                # Base con estacionalidad
                base_occ = 48.0
                if month in [1, 6, 7, 12]:
                    base_occ += 18.0
                elif month in [3, 4, 10]:
                    base_occ += 8.0

                # COVID
                if year == 2020 and month >= 3:
                    base_occ *= 0.25
                elif year == 2021 and month <= 6:
                    base_occ *= 0.6

                # Departamentos turísticos tienen más ocupación
                if dept in ["San Andrés", "Bolívar", "Magdalena"]:
                    base_occ += 8

                occ = min(95, max(8, base_occ + np.random.normal(0, 6)))
                rooms = np.random.randint(800, 6000)

                records.append({
                    "anio": year,
                    "mes": month,
                    "departamento": dept,
                    "porcentaje_ocupacion": round(occ, 1),
                    "habitaciones_disponibles": rooms,
                    "habitaciones_ocupadas": int(rooms * occ / 100),
                    "tarifa_promedio_cop": round(
                        np.random.uniform(90000, 450000), 0
                    ),
                })

    df = pd.DataFrame(records)
    output_path = output_dir / "ocupacion_hotelera_sintetico.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"   ✓ Ocupación: {output_path} ({len(df)} registros)")


if __name__ == "__main__":
    main()
