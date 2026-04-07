import streamlit as st
import plotly.express as px
from core import init_filters, render_filters, build_where, run_query

st.set_page_config(layout="wide")

# ===== GLOBAL =====
init_filters()
render_filters()

filters = {
    "shift": st.session_state.shift,
    "payment": st.session_state.payment,
    "vendor": st.session_state.vendor,
    "hour_range": st.session_state.hour_range,
    "days": st.session_state.days
}

where, params = build_where(filters)

# ===== DASHBOARD DESEMPENHO FORNECEDOR =====
@st.cache_data
def get_kpis(where, params):
    return run_query(f"""
        SELECT 
            COUNT(*) AS total_trips,
            AVG(tip_amount) AS avg_tip,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_revenue,
            AVG(speed_mph) AS avg_speed
        FROM fct_trips
        {where}
    """, params)

@st.cache_data
def get_kpis_vendor(where, params):
    return run_query(f"""
        SELECT  
            VendorID,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_revenue,
            AVG(speed_mph) AS avg_speed
        FROM fct_trips
        {where}
        GROUP BY
            VendorID
    """, params)

@st.cache_data
def get_heatmap_speed(where, params):
    return run_query(f"""
        SELECT  
            trip_hour,
            day_name,
            AVG(speed_mph) AS avg_speed
        FROM fct_trips
        {where}
        GROUP BY
            trip_hour,
            day_of_week,
            day_name
        ORDER BY
            day_of_week DESC 
    """, params)

kpis = get_kpis(where, params)
kpis_vendor = get_kpis_vendor(where, params)
heatmap = get_heatmap_speed(where, params)

# ===== UI =====
st.title("Dashboard NYC Taxi Trips")
st.subheader("Desempenho Fornecedor")

row = kpis.iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total de Viagens", f"{row['total_trips']:,}")
col2.metric("Receita Total", f"${row['total_revenue']:,.2f}")
col3.metric("Média de Gorjeta", f"${row['avg_tip']:,.2f}")
col4.metric("Ticket Médio", f"${row['avg_revenue']:,.2f}")
col5.metric("Velocidade Média", f"{row['avg_speed']:.2f} mph")

col6, col7 = st.columns(2)

col6.subheader("Viagens por Fornecedor")
col6.bar_chart(kpis_vendor.set_index("VendorID")["total_trips"], sort=True)
col7.subheader("Receita por Fornecedor")
col7.bar_chart(kpis_vendor.set_index("VendorID")["total_revenue"], sort=True)

fig = px.density_heatmap(
    heatmap,
    x="trip_hour",
    y="day_name",
    z="avg_speed",
    color_continuous_scale="Blues",
    labels={
        "trip_hour": "Hora do Dia",
        "day_name": "Dia da Semana",
        "avg_speed": "Média de Velocidade (mph)"
    }
)

st.subheader("Heatmap de Velocidade (mph) por Dia e Hora")
st.plotly_chart(fig, use_container_width=True)