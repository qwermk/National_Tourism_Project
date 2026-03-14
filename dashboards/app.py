# =============================================================================
# 📊 Dashboard de Turismo Colombia — Streamlit
# =============================================================================
#
# Dashboard interactivo con 8 pestañas:
#   1. Llegadas de Turistas   — YoY, motivo, punto de entrada, top países
#   2. Ocupación Hotelera     — tendencia, ranking, RevPAR
#   3. PIB Turístico          — empleo, correlación PIB vs ocupación
#   4. Flujos Migratorios     — balance neto, top nacionalidades
#   5. Transporte Aéreo       — pasajeros por aeropuerto (Aerocivil)
#   6. Balanza Turística      — ingresos vs egresos (BanRep)
#   7. Comparación Regional   — Colombia vs vecinos (World Bank)
#   8. Descargas              — exportar datos a CSV
#
# Ejecutar:  streamlit run dashboards/app.py
# =============================================================================

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Configuración de la página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Turismo Colombia",
    page_icon="🇨🇴",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Conexión a DuckDB
# ---------------------------------------------------------------------------
DUCKDB_PATH = os.getenv(
    "DUCKDB_DATABASE",
    str(Path(__file__).parent.parent / "data" / "tourism.duckdb"),
)


@st.cache_resource
def get_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return conn.execute(sql).fetchdf()


# ---------------------------------------------------------------------------
# Verificar tablas Gold disponibles
# ---------------------------------------------------------------------------
_GOLD_TABLES = [
    "gold.fct_tourism_arrivals",
    "gold.fct_hotel_occupancy",
    "gold.fct_tourism_gdp",
    "gold.fct_migration_flows",
    "gold.fct_air_passengers",
    "gold.fct_tourism_balance",
    "gold.fct_world_bank_comparison",
    "gold.dim_departments",
    "gold.dim_date",
]


def check_tables_exist() -> dict:
    tables = {}
    for table_name in _GOLD_TABLES:
        try:
            run_query(f"SELECT 1 FROM {table_name} LIMIT 1")
            tables[table_name] = True
        except Exception:
            tables[table_name] = False
    return tables


# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------
st.sidebar.title("🇨🇴 Turismo Colombia")
st.sidebar.markdown("---")
st.sidebar.markdown("### Filtros")

try:
    available_tables = check_tables_exist()

    if available_tables.get("gold.fct_tourism_arrivals"):
        years_df = run_query(
            "SELECT DISTINCT year FROM gold.fct_tourism_arrivals ORDER BY year"
        )
        all_years = years_df["year"].tolist()
    else:
        all_years = list(range(2010, 2027))

    year_range = st.sidebar.slider(
        "Rango de años",
        min_value=min(all_years),
        max_value=max(all_years),
        value=(min(all_years), max(all_years)),
        help="Filtra los datos por rango de años",
    )

    if available_tables.get("gold.fct_tourism_arrivals"):
        depts_df = run_query(
            "SELECT DISTINCT destination_department "
            "FROM gold.fct_tourism_arrivals ORDER BY 1"
        )
        departments = ["Todos"] + depts_df["destination_department"].tolist()
    else:
        departments = ["Todos"]

    selected_dept = st.sidebar.selectbox(
        "Departamento", departments, help="Filtra por departamento de destino"
    )

except Exception as e:
    st.sidebar.error(f"Error conectando a DuckDB: {e}")
    st.sidebar.info(
        "Asegúrate de haber ejecutado el pipeline Dagster + dbt "
        "para generar las tablas Gold."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Nombres de meses
# ---------------------------------------------------------------------------
_MONTH_NAMES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}

# Coordenadas aproximadas de capitales departamentales para el mapa
_DEPT_COORDS = {
    "Bogotá D.C.": (4.711, -74.072), "Antioquia": (6.252, -75.564),
    "Valle Del Cauca": (3.452, -76.532), "Bolívar": (10.400, -75.514),
    "Atlántico": (10.964, -74.781), "Santander": (7.130, -73.126),
    "San Andrés": (12.584, -81.701), "Magdalena": (11.241, -74.199),
    "Nariño": (1.287, -77.391), "Risaralda": (4.814, -75.696),
    "Quindío": (4.534, -75.681), "Boyacá": (5.534, -73.362),
    "Cundinamarca": (4.983, -74.067), "Norte De Santander": (7.894, -72.508),
    "Caldas": (5.066, -75.517), "Tolima": (4.438, -75.232),
    "Huila": (2.927, -75.282), "Cesar": (10.473, -73.253),
    "Meta": (4.153, -73.636), "Cauca": (2.442, -76.606),
    "Sucre": (9.304, -75.398), "Córdoba": (8.748, -75.881),
    "La Guajira": (11.544, -72.907), "Casanare": (5.337, -72.395),
    "Amazonas": (-1.012, -71.984), "Putumayo": (1.152, -76.652),
    "Chocó": (5.691, -76.658), "Arauca": (7.090, -70.762),
    "Caquetá": (1.614, -75.612), "Guaviare": (2.570, -72.641),
    "Vaupés": (1.198, -70.174), "Vichada": (4.423, -69.750),
    "Guainía": (3.865, -67.924),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def year_filter(col: str = "year") -> str:
    return f"{col} BETWEEN {year_range[0]} AND {year_range[1]}"


def dept_filter(col: str = "destination_department") -> str:
    if selected_dept == "Todos":
        return ""
    return f"AND {col} = '{selected_dept}'"


def download_csv(df: pd.DataFrame, filename: str, label: str = "📥 Descargar CSV"):
    """Render a download button for a DataFrame."""
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label, csv, filename, "text/csv")


# ===========================================================================
# PÁGINA PRINCIPAL — KPIs
# ===========================================================================
st.title("Dashboard de Turismo — Colombia")
st.markdown(
    "Análisis interactivo del turismo en Colombia. "
    "Datos actualizados por el pipeline **Dagster + dbt + DuckDB**."
)
st.markdown("---")

if available_tables.get("gold.fct_tourism_arrivals"):
    kpi_df = run_query(f"""
        SELECT
            SUM(total_visitors) as total_visitors,
            ROUND(SUM(total_spending_usd), 0) as total_spending_usd,
            COUNT(DISTINCT country_of_origin) as countries_of_origin,
            COUNT(DISTINCT destination_department) as departments
        FROM gold.fct_tourism_arrivals
        WHERE {year_filter()} {dept_filter()}
    """)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Visitantes", f"{int(kpi_df['total_visitors'].iloc[0]):,}")
    with c2:
        st.metric("Gasto Total (USD)", f"${int(kpi_df['total_spending_usd'].iloc[0]):,}")
    with c3:
        st.metric("Países de Origen", int(kpi_df["countries_of_origin"].iloc[0]))
    with c4:
        st.metric("Departamentos", int(kpi_df["departments"].iloc[0]))

st.markdown("---")

# ===========================================================================
# TABS
# ===========================================================================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📈 Llegadas",
    "🏨 Ocupación",
    "💰 PIB Turístico",
    "✈️ Migraciones",
    "🛫 Transporte Aéreo",
    "💱 Balanza Turística",
    "🌎 Comparación Regional",
    "📥 Descargas",
])

# ===========================================================================
# TAB 1: Llegadas de Turistas (mejorado)
# ===========================================================================
with tab1:
    st.header("Llegadas de Turistas Internacionales")

    if available_tables.get("gold.fct_tourism_arrivals"):
        # --- Visitantes por año + YoY ---
        arrivals_year = run_query(f"""
            SELECT year,
                   SUM(total_visitors)          as total_visitors,
                   ROUND(SUM(total_spending_usd), 0) as total_spending_usd
            FROM gold.fct_tourism_arrivals
            WHERE {year_filter()} {dept_filter()}
            GROUP BY year ORDER BY year
        """)

        if len(arrivals_year) > 1:
            arrivals_year["yoy_pct"] = arrivals_year["total_visitors"].pct_change() * 100

        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                arrivals_year, x="year", y="total_visitors",
                title="Total de Visitantes por Año",
                labels={"year": "Año", "total_visitors": "Visitantes"},
                color_discrete_sequence=["#1f77b4"],
            )
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            if "yoy_pct" in arrivals_year.columns:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=arrivals_year["year"], y=arrivals_year["yoy_pct"],
                    marker_color=[
                        "#2ca02c" if v >= 0 else "#d62728"
                        for v in arrivals_year["yoy_pct"].fillna(0)
                    ],
                ))
                fig.update_layout(
                    title="Crecimiento Interanual (%)",
                    xaxis_title="Año", yaxis_title="Variación %",
                )
                fig.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig, use_container_width=True)

        # --- Gasto per cápita + Top países ---
        st.subheader("Top 10 Países de Origen")
        top_countries = run_query(f"""
            SELECT country_of_origin,
                   SUM(total_visitors) as total_visitors,
                   ROUND(AVG(average_spending_usd), 2) as avg_spending_usd
            FROM gold.fct_tourism_arrivals
            WHERE {year_filter()} {dept_filter()}
            GROUP BY country_of_origin
            ORDER BY total_visitors DESC LIMIT 10
        """)

        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                top_countries, x="total_visitors", y="country_of_origin",
                orientation="h", title="Principales Países de Origen",
                labels={"country_of_origin": "País", "total_visitors": "Visitantes"},
                color_discrete_sequence=["#2ca02c"],
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.bar(
                top_countries, x="avg_spending_usd", y="country_of_origin",
                orientation="h", title="Gasto Promedio per Cápita (USD)",
                labels={"country_of_origin": "País", "avg_spending_usd": "USD"},
                color_discrete_sequence=["#ff7f0e"],
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

        # --- Distribución por Motivo de Viaje ---
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Motivo de Viaje")
            purpose_df = run_query(f"""
                SELECT travel_purpose, SUM(total_visitors) as total
                FROM gold.fct_tourism_arrivals
                WHERE {year_filter()} {dept_filter()}
                GROUP BY travel_purpose ORDER BY total DESC
            """)
            if not purpose_df.empty:
                fig = px.pie(
                    purpose_df, values="total", names="travel_purpose",
                    title="Distribución por Motivo de Viaje",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("Punto de Entrada")
            entry_df = run_query(f"""
                SELECT entry_point, SUM(total_visitors) as total
                FROM gold.fct_tourism_arrivals
                WHERE {year_filter()} {dept_filter()}
                GROUP BY entry_point ORDER BY total DESC
            """)
            if not entry_df.empty:
                fig = px.pie(
                    entry_df, values="total", names="entry_point",
                    title="Distribución por Punto de Entrada",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                st.plotly_chart(fig, use_container_width=True)

        # --- Estacionalidad ---
        st.subheader("Estacionalidad del Turismo")
        seasonality = run_query(f"""
            SELECT month, SUM(total_visitors) as total_visitors
            FROM gold.fct_tourism_arrivals
            WHERE {year_filter()} {dept_filter()}
            GROUP BY month ORDER BY month
        """)
        seasonality["month_name"] = seasonality["month"].map(_MONTH_NAMES)
        fig = px.bar(
            seasonality, x="month_name", y="total_visitors",
            title="Visitantes por Mes", color_discrete_sequence=["#ff7f0e"],
            labels={"month_name": "Mes", "total_visitors": "Visitantes"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- Mapa de departamentos ---
        st.subheader("🗺️ Mapa de Visitantes por Departamento")
        dept_map = run_query(f"""
            SELECT destination_department as dept,
                   SUM(total_visitors) as visitors
            FROM gold.fct_tourism_arrivals
            WHERE {year_filter()}
            GROUP BY destination_department
        """)
        if not dept_map.empty:
            dept_map["lat"] = dept_map["dept"].map(
                lambda d: _DEPT_COORDS.get(d, (4.5, -74.0))[0]
            )
            dept_map["lon"] = dept_map["dept"].map(
                lambda d: _DEPT_COORDS.get(d, (4.5, -74.0))[1]
            )
            fig = px.scatter_geo(
                dept_map, lat="lat", lon="lon", size="visitors",
                hover_name="dept", color="visitors",
                title="Visitantes por Departamento",
                color_continuous_scale="YlOrRd",
                scope="south america",
                size_max=40,
            )
            fig.update_geos(
                center=dict(lat=4.5, lon=-74),
                projection_scale=6,
                visible=True,
            )
            st.plotly_chart(fig, use_container_width=True)

        download_csv(arrivals_year, "llegadas_turistas.csv")
    else:
        st.warning("Tabla gold.fct_tourism_arrivals no encontrada.")

# ===========================================================================
# TAB 2: Ocupación Hotelera (mejorado)
# ===========================================================================
with tab2:
    st.header("Ocupación Hotelera por Departamento")

    if available_tables.get("gold.fct_hotel_occupancy"):
        occ_depts = run_query(
            "SELECT DISTINCT department FROM gold.fct_hotel_occupancy ORDER BY 1"
        )
        selected_occ_dept = st.selectbox(
            "Selecciona departamento:",
            ["Todos"] + occ_depts["department"].tolist(),
            key="occ_dept",
        )
        dept_clause = (
            f"AND department = '{selected_occ_dept}'"
            if selected_occ_dept != "Todos" else ""
        )

        # --- Tendencia ---
        occ_trend = run_query(f"""
            SELECT year, month, department,
                   ROUND(AVG(occupancy_rate), 1) as avg_occupancy
            FROM gold.fct_hotel_occupancy
            WHERE {year_filter()} {dept_clause}
            GROUP BY year, month, department ORDER BY year, month
        """)
        if not occ_trend.empty:
            occ_trend["fecha"] = pd.to_datetime(
                occ_trend["year"].astype(str) + "-"
                + occ_trend["month"].astype(str).str.zfill(2) + "-01"
            )
            fig = px.line(
                occ_trend, x="fecha", y="avg_occupancy", color="department",
                title="Tendencia de Ocupación Hotelera (%)",
                labels={"fecha": "Fecha", "avg_occupancy": "Ocupación (%)", "department": "Depto"},
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Ranking + RevPAR ---
        st.subheader("Ranking y RevPAR por Departamento")
        occ_summary = run_query(f"""
            SELECT department,
                   ROUND(AVG(occupancy_rate), 1) as avg_occupancy,
                   ROUND(AVG(average_rate_cop), 0) as avg_rate_cop,
                   SUM(available_rooms) as total_rooms,
                   ROUND(AVG(occupancy_rate) * AVG(average_rate_cop) / 100, 0) as revpar_cop
            FROM gold.fct_hotel_occupancy
            WHERE {year_filter()}
            GROUP BY department ORDER BY avg_occupancy DESC
        """)

        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                occ_summary.head(15), x="avg_occupancy", y="department",
                orientation="h", title="Ranking Ocupación (%)",
                labels={"department": "Departamento", "avg_occupancy": "Ocupación %"},
                color="avg_occupancy", color_continuous_scale="Greens",
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.bar(
                occ_summary.head(15), x="revpar_cop", y="department",
                orientation="h", title="RevPAR Estimado (COP)",
                labels={"department": "Departamento", "revpar_cop": "RevPAR (COP)"},
                color="revpar_cop", color_continuous_scale="Blues",
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(occ_summary, use_container_width=True, hide_index=True)
        download_csv(occ_summary, "ocupacion_hotelera.csv")
    else:
        st.warning("Tabla gold.fct_hotel_occupancy no encontrada.")

# ===========================================================================
# TAB 3: PIB Turístico (mejorado)
# ===========================================================================
with tab3:
    st.header("PIB Turístico de Colombia")

    if available_tables.get("gold.fct_tourism_gdp"):
        gdp_df = run_query(f"""
            SELECT * FROM gold.fct_tourism_gdp
            WHERE {year_filter()} ORDER BY year
        """)
        if not gdp_df.empty:
            latest = gdp_df.iloc[-1]
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("PIB Turístico (último año)",
                          f"${latest['tourism_gdp_billions_cop']:.1f}B COP")
            with c2:
                st.metric("% del PIB Total", f"{latest['pct_of_total_gdp']:.1f}%")
            with c3:
                st.metric("Empleo Turístico",
                          f"{int(latest['tourism_employment_thousands'])}K empleos")

            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(
                    gdp_df, x="year", y="tourism_gdp_billions_cop",
                    title="PIB Turístico (Billones COP)",
                    labels={"year": "Año", "tourism_gdp_billions_cop": "PIB (B COP)"},
                    color_discrete_sequence=["#9467bd"],
                )
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                fig = px.line(
                    gdp_df, x="year", y="annual_variation_pct",
                    title="Variación Anual del PIB Turístico (%)",
                    labels={"year": "Año", "annual_variation_pct": "Variación %"},
                    markers=True,
                )
                fig.add_hline(y=0, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

            # --- Tendencia de empleo ---
            st.subheader("Tendencia de Empleo Turístico")
            fig = px.area(
                gdp_df, x="year", y="tourism_employment_thousands",
                title="Empleo en el Sector Turístico (miles)",
                labels={"year": "Año", "tourism_employment_thousands": "Empleos (miles)"},
                color_discrete_sequence=["#17becf"],
            )
            st.plotly_chart(fig, use_container_width=True)

            # --- Correlación PIB vs Ocupación ---
            if available_tables.get("gold.fct_hotel_occupancy"):
                st.subheader("Correlación: PIB Turístico vs Ocupación Hotelera")
                occ_annual = run_query(f"""
                    SELECT year, ROUND(AVG(occupancy_rate), 1) as avg_occupancy
                    FROM gold.fct_hotel_occupancy
                    WHERE {year_filter()}
                    GROUP BY year
                """)
                merged = gdp_df.merge(occ_annual, on="year", how="inner")
                if not merged.empty:
                    fig = px.scatter(
                        merged, x="avg_occupancy", y="tourism_gdp_billions_cop",
                        text="year", trendline="ols",
                        title="PIB Turístico vs Ocupación Hotelera",
                        labels={
                            "avg_occupancy": "Ocupación Promedio (%)",
                            "tourism_gdp_billions_cop": "PIB Turístico (B COP)",
                        },
                        color_discrete_sequence=["#e377c2"],
                    )
                    fig.update_traces(textposition="top center")
                    st.plotly_chart(fig, use_container_width=True)

            st.subheader("Datos Completos")
            st.dataframe(gdp_df, use_container_width=True, hide_index=True)
            download_csv(gdp_df, "pib_turistico.csv")
    else:
        st.warning("Tabla gold.fct_tourism_gdp no encontrada.")

# ===========================================================================
# TAB 4: Flujos Migratorios (mejorado + balance)
# ===========================================================================
with tab4:
    st.header("Flujos Migratorios de Colombia")

    if available_tables.get("gold.fct_migration_flows"):
        movement_type = st.radio(
            "Tipo de movimiento:", ["Todos", "Entrada", "Salida"],
            horizontal=True,
        )
        movement_clause = ""
        if movement_type == "Entrada":
            movement_clause = "AND movement_type = 'entrada'"
        elif movement_type == "Salida":
            movement_clause = "AND movement_type = 'salida'"

        # --- Flujos por año ---
        flows_year = run_query(f"""
            SELECT year, movement_type, SUM(number_of_travelers) as total_travelers
            FROM gold.fct_migration_flows
            WHERE {year_filter()} {movement_clause}
            GROUP BY year, movement_type ORDER BY year
        """)
        if not flows_year.empty:
            fig = px.bar(
                flows_year, x="year", y="total_travelers", color="movement_type",
                title="Flujos Migratorios por Año", barmode="group",
                labels={"year": "Año", "total_travelers": "Viajeros", "movement_type": "Tipo"},
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Balance migratorio neto ---
        st.subheader("Balance Migratorio Neto (Entradas − Salidas)")
        balance_df = run_query(f"""
            SELECT year,
                   SUM(CASE WHEN movement_type = 'entrada' THEN number_of_travelers ELSE 0 END) as entradas,
                   SUM(CASE WHEN movement_type = 'salida'  THEN number_of_travelers ELSE 0 END) as salidas
            FROM gold.fct_migration_flows
            WHERE {year_filter()}
            GROUP BY year ORDER BY year
        """)
        if not balance_df.empty:
            balance_df["balance"] = balance_df["entradas"] - balance_df["salidas"]
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=balance_df["year"], y=balance_df["balance"],
                marker_color=[
                    "#2ca02c" if v >= 0 else "#d62728" for v in balance_df["balance"]
                ],
                name="Balance",
            ))
            fig.update_layout(
                title="Balance Migratorio Neto por Año",
                xaxis_title="Año", yaxis_title="Balance (entradas − salidas)",
            )
            fig.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig, use_container_width=True)

        # --- Top nacionalidades ---
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top 10 Nacionalidades")
            top_nat = run_query(f"""
                SELECT nationality, movement_type,
                       SUM(number_of_travelers) as total_travelers
                FROM gold.fct_migration_flows
                WHERE {year_filter()} {movement_clause}
                GROUP BY nationality, movement_type
                ORDER BY total_travelers DESC LIMIT 10
            """)
            if not top_nat.empty:
                fig = px.bar(
                    top_nat, x="total_travelers", y="nationality",
                    color="movement_type", orientation="h",
                    title="Principales Nacionalidades",
                    labels={"nationality": "Nacionalidad", "total_travelers": "Viajeros"},
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("Puntos de Control")
            points_df = run_query(f"""
                SELECT control_point, SUM(number_of_travelers) as total_travelers
                FROM gold.fct_migration_flows
                WHERE {year_filter()} {movement_clause}
                GROUP BY control_point ORDER BY total_travelers DESC
            """)
            if not points_df.empty:
                fig = px.pie(
                    points_df, values="total_travelers", names="control_point",
                    title="Distribución por Punto de Control",
                )
                st.plotly_chart(fig, use_container_width=True)

        download_csv(flows_year, "flujos_migratorios.csv")
    else:
        st.warning("Tabla gold.fct_migration_flows no encontrada.")

# ===========================================================================
# TAB 5: Transporte Aéreo (NUEVO — Aerocivil)
# ===========================================================================
with tab5:
    st.header("🛫 Transporte Aéreo — Pasajeros por Aeropuerto")

    if available_tables.get("gold.fct_air_passengers"):
        # KPIs
        air_kpi = run_query(f"""
            SELECT SUM(total_passengers) as total,
                   SUM(domestic_passengers) as domestic,
                   SUM(international_passengers) as international,
                   COUNT(DISTINCT airport) as airports
            FROM gold.fct_air_passengers
            WHERE {year_filter()}
        """)
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Pasajeros", f"{int(air_kpi['total'].iloc[0]):,}")
        with c2:
            st.metric("Nacionales", f"{int(air_kpi['domestic'].iloc[0]):,}")
        with c3:
            st.metric("Internacionales", f"{int(air_kpi['international'].iloc[0]):,}")
        with c4:
            st.metric("Aeropuertos", int(air_kpi["airports"].iloc[0]))

        # --- Tendencia anual ---
        air_annual = run_query(f"""
            SELECT year,
                   SUM(domestic_passengers) as domestic,
                   SUM(international_passengers) as international,
                   SUM(total_passengers) as total
            FROM gold.fct_air_passengers
            WHERE {year_filter()}
            GROUP BY year ORDER BY year
        """)
        fig = go.Figure()
        fig.add_trace(go.Bar(x=air_annual["year"], y=air_annual["domestic"],
                             name="Nacionales", marker_color="#1f77b4"))
        fig.add_trace(go.Bar(x=air_annual["year"], y=air_annual["international"],
                             name="Internacionales", marker_color="#ff7f0e"))
        fig.update_layout(
            title="Pasajeros Aéreos por Año", barmode="stack",
            xaxis_title="Año", yaxis_title="Pasajeros",
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- Top aeropuertos ---
        st.subheader("Top Aeropuertos")
        top_airports = run_query(f"""
            SELECT airport, airport_city,
                   SUM(total_passengers) as total,
                   SUM(international_passengers) as international,
                   ROUND(AVG(pct_international), 1) as pct_intl
            FROM gold.fct_air_passengers
            WHERE {year_filter()}
            GROUP BY airport, airport_city
            ORDER BY total DESC
        """)
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                top_airports, x="total", y="airport", orientation="h",
                title="Pasajeros Totales por Aeropuerto",
                labels={"airport": "Aeropuerto", "total": "Pasajeros"},
                color="pct_intl", color_continuous_scale="RdYlGn",
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.bar(
                top_airports, x="pct_intl", y="airport", orientation="h",
                title="% Pasajeros Internacionales",
                labels={"airport": "Aeropuerto", "pct_intl": "% Internacional"},
                color_discrete_sequence=["#9467bd"],
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

        # --- Estacionalidad aérea ---
        st.subheader("Estacionalidad del Tráfico Aéreo")
        air_monthly = run_query(f"""
            SELECT month, SUM(total_passengers) as total
            FROM gold.fct_air_passengers
            WHERE {year_filter()}
            GROUP BY month ORDER BY month
        """)
        air_monthly["month_name"] = air_monthly["month"].map(_MONTH_NAMES)
        fig = px.bar(
            air_monthly, x="month_name", y="total",
            title="Pasajeros por Mes", color_discrete_sequence=["#17becf"],
            labels={"month_name": "Mes", "total": "Pasajeros"},
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(top_airports, use_container_width=True, hide_index=True)
        download_csv(top_airports, "pasajeros_aereos.csv")
    else:
        st.warning(
            "Tabla gold.fct_air_passengers no encontrada. "
            "Materializa raw_aerocivil_passengers y ejecuta dbt build."
        )

# ===========================================================================
# TAB 6: Balanza Turística (NUEVO — Banco de la República)
# ===========================================================================
with tab6:
    st.header("💱 Balanza Turística de Colombia")

    if available_tables.get("gold.fct_tourism_balance"):
        bal_df = run_query(f"""
            SELECT * FROM gold.fct_tourism_balance
            WHERE {year_filter()} ORDER BY year, quarter
        """)
        if not bal_df.empty:
            # KPIs del último año completo
            bal_annual = run_query(f"""
                SELECT year,
                       SUM(tourism_income_usd_millions) as income,
                       SUM(tourism_expenditure_usd_millions) as expenditure,
                       SUM(tourism_balance_usd_millions) as balance
                FROM gold.fct_tourism_balance
                WHERE {year_filter()}
                GROUP BY year ORDER BY year
            """)
            if not bal_annual.empty:
                latest_bal = bal_annual.iloc[-1]
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("Ingresos (último año)",
                              f"${latest_bal['income']:,.0f}M USD")
                with c2:
                    st.metric("Egresos (último año)",
                              f"${latest_bal['expenditure']:,.0f}M USD")
                with c3:
                    bal_val = latest_bal['balance']
                    st.metric("Balance Neto",
                              f"${bal_val:,.0f}M USD",
                              delta=f"{'Superávit' if bal_val > 0 else 'Déficit'}")

                # --- Tendencia anual ---
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=bal_annual["year"], y=bal_annual["income"],
                    name="Ingresos", marker_color="#2ca02c",
                ))
                fig.add_trace(go.Bar(
                    x=bal_annual["year"], y=bal_annual["expenditure"],
                    name="Egresos", marker_color="#d62728",
                ))
                fig.add_trace(go.Scatter(
                    x=bal_annual["year"], y=bal_annual["balance"],
                    name="Balance", mode="lines+markers",
                    line=dict(color="#1f77b4", width=3),
                ))
                fig.update_layout(
                    title="Balanza Turística Anual (USD Millones)",
                    xaxis_title="Año", yaxis_title="USD Millones",
                    barmode="group",
                )
                fig.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig, use_container_width=True)

            # --- Tendencia trimestral ---
            st.subheader("Detalle Trimestral")
            bal_df["periodo"] = (
                bal_df["year"].astype(str) + "-Q" + bal_df["quarter"].astype(str)
            )
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=bal_df["periodo"], y=bal_df["tourism_income_usd_millions"],
                name="Ingresos", mode="lines+markers",
            ))
            fig.add_trace(go.Scatter(
                x=bal_df["periodo"], y=bal_df["tourism_expenditure_usd_millions"],
                name="Egresos", mode="lines+markers",
            ))
            fig.update_layout(
                title="Ingresos vs Egresos Trimestrales (USD M)",
                xaxis_title="Periodo", yaxis_title="USD Millones",
            )
            st.plotly_chart(fig, use_container_width=True)

            download_csv(bal_annual, "balanza_turistica.csv")
    else:
        st.warning(
            "Tabla gold.fct_tourism_balance no encontrada. "
            "Materializa raw_banrep_tourism_balance y ejecuta dbt build."
        )

# ===========================================================================
# TAB 7: Comparación Regional (NUEVO — World Bank)
# ===========================================================================
with tab7:
    st.header("🌎 Comparación Regional — Colombia vs Vecinos")

    if available_tables.get("gold.fct_world_bank_comparison"):
        # --- Llegadas internacionales comparadas ---
        arrivals_comp = run_query(f"""
            SELECT year, country_name, value
            FROM gold.fct_world_bank_comparison
            WHERE indicator_code = 'ST.INT.ARVL'
            AND {year_filter()}
            ORDER BY year, country_name
        """)
        if not arrivals_comp.empty:
            fig = px.line(
                arrivals_comp, x="year", y="value", color="country_name",
                title="Llegadas Internacionales por País",
                labels={"year": "Año", "value": "Llegadas", "country_name": "País"},
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Ingresos por turismo comparados ---
        receipts_comp = run_query(f"""
            SELECT year, country_name, ROUND(value / 1e6, 1) as value_millions
            FROM gold.fct_world_bank_comparison
            WHERE indicator_code = 'ST.INT.RCPT.CD'
            AND {year_filter()}
            ORDER BY year, country_name
        """)
        if not receipts_comp.empty:
            fig = px.line(
                receipts_comp, x="year", y="value_millions", color="country_name",
                title="Ingresos por Turismo (USD Millones)",
                labels={
                    "year": "Año",
                    "value_millions": "USD Millones",
                    "country_name": "País",
                },
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Turismo como % exportaciones ---
        exports_comp = run_query(f"""
            SELECT year, country_name, value as pct_exports
            FROM gold.fct_world_bank_comparison
            WHERE indicator_code = 'ST.INT.RCPT.XP.ZS'
            AND {year_filter()}
            ORDER BY year, country_name
        """)
        if not exports_comp.empty:
            fig = px.line(
                exports_comp, x="year", y="pct_exports", color="country_name",
                title="Turismo como % de Exportaciones",
                labels={
                    "year": "Año",
                    "pct_exports": "% Exportaciones",
                    "country_name": "País",
                },
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Tabla resumen último año disponible ---
        st.subheader("Resumen por País (último año disponible)")
        summary_comp = run_query(f"""
            WITH latest AS (
                SELECT country_name, indicator_code, value,
                       ROW_NUMBER() OVER (
                           PARTITION BY country_name, indicator_code
                           ORDER BY year DESC
                       ) as rn
                FROM gold.fct_world_bank_comparison
                WHERE {year_filter()}
            )
            SELECT country_name,
                   MAX(CASE WHEN indicator_code = 'ST.INT.ARVL' THEN value END) as arrivals,
                   MAX(CASE WHEN indicator_code = 'ST.INT.RCPT.CD'
                       THEN ROUND(value / 1e6, 1) END) as receipts_usd_m,
                   MAX(CASE WHEN indicator_code = 'ST.INT.RCPT.XP.ZS'
                       THEN ROUND(value, 1) END) as pct_exports
            FROM latest WHERE rn = 1
            GROUP BY country_name
            ORDER BY arrivals DESC
        """)
        if not summary_comp.empty:
            st.dataframe(
                summary_comp, use_container_width=True, hide_index=True,
                column_config={
                    "country_name": "País",
                    "arrivals": st.column_config.NumberColumn(
                        "Llegadas", format="%d"
                    ),
                    "receipts_usd_m": st.column_config.NumberColumn(
                        "Ingresos (USD M)", format="$%.1f"
                    ),
                    "pct_exports": st.column_config.NumberColumn(
                        "% Exportaciones", format="%.1f%%"
                    ),
                },
            )
            download_csv(summary_comp, "comparacion_regional.csv")
    else:
        st.warning(
            "Tabla gold.fct_world_bank_comparison no encontrada. "
            "Materializa raw_world_bank_regional y ejecuta dbt build."
        )

# ===========================================================================
# TAB 8: Descargas (exportar datos a CSV)
# ===========================================================================
with tab8:
    st.header("📥 Descarga de Datos")
    st.markdown(
        "Descarga datasets completos de las tablas Gold en formato CSV. "
        "Los datos respetan los filtros de año seleccionados."
    )

    datasets = {
        "Llegadas de Turistas": {
            "table": "gold.fct_tourism_arrivals",
            "filename": "fct_tourism_arrivals.csv",
            "sql": f"""
                SELECT * FROM gold.fct_tourism_arrivals
                WHERE {year_filter()} {dept_filter()}
                ORDER BY year, month
            """,
        },
        "Ocupación Hotelera": {
            "table": "gold.fct_hotel_occupancy",
            "filename": "fct_hotel_occupancy.csv",
            "sql": f"""
                SELECT * FROM gold.fct_hotel_occupancy
                WHERE {year_filter()} ORDER BY year, month
            """,
        },
        "PIB Turístico": {
            "table": "gold.fct_tourism_gdp",
            "filename": "fct_tourism_gdp.csv",
            "sql": f"""
                SELECT * FROM gold.fct_tourism_gdp
                WHERE {year_filter()} ORDER BY year
            """,
        },
        "Flujos Migratorios": {
            "table": "gold.fct_migration_flows",
            "filename": "fct_migration_flows.csv",
            "sql": f"""
                SELECT * FROM gold.fct_migration_flows
                WHERE {year_filter()} ORDER BY year, month
            """,
        },
        "Pasajeros Aéreos": {
            "table": "gold.fct_air_passengers",
            "filename": "fct_air_passengers.csv",
            "sql": f"""
                SELECT * FROM gold.fct_air_passengers
                WHERE {year_filter()} ORDER BY year, month
            """,
        },
        "Balanza Turística": {
            "table": "gold.fct_tourism_balance",
            "filename": "fct_tourism_balance.csv",
            "sql": f"""
                SELECT * FROM gold.fct_tourism_balance
                WHERE {year_filter()} ORDER BY year, quarter
            """,
        },
        "Comparación Regional": {
            "table": "gold.fct_world_bank_comparison",
            "filename": "fct_world_bank_comparison.csv",
            "sql": f"""
                SELECT * FROM gold.fct_world_bank_comparison
                WHERE {year_filter()} ORDER BY country_name, indicator_code, year
            """,
        },
        "Departamentos (dimensión)": {
            "table": "gold.dim_departments",
            "filename": "dim_departments.csv",
            "sql": "SELECT * FROM gold.dim_departments ORDER BY department",
        },
    }

    for label, info in datasets.items():
        if available_tables.get(info["table"], False):
            with st.expander(f"📊 {label}"):
                try:
                    df = run_query(info["sql"])
                    st.write(f"**{len(df):,}** registros disponibles")
                    st.dataframe(df.head(10), use_container_width=True, hide_index=True)
                    download_csv(df, info["filename"], f"📥 Descargar {label}")
                except Exception as e:
                    st.error(f"Error consultando {label}: {e}")
        else:
            with st.expander(f"⚠️ {label} (no disponible)"):
                st.info(
                    f"La tabla `{info['table']}` aún no está disponible. "
                    "Ejecuta el pipeline para generarla."
                )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    "📊 **National Tourism Project** — Pipeline: Dagster + dbt + DuckDB | "
    "Dashboard: Streamlit + Plotly | "
    "Fuentes: CITUR, DANE, Migración Colombia, World Bank, Aerocivil, BanRep"
)
