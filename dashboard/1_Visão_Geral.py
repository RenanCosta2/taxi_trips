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

# ===== DASHBOARD VISÃO GERAL =====
@st.cache_data
def get_kpis(where, params):
    return run_query(f"""
        SELECT 
            COUNT(*) AS total_trips,
            AVG(trip_distance) AS avg_mile_distance,
            AVG(trip_duration_min) AS avg_duration_min,
            AVG(total_amount) AS avg_revenue,
            SUM(total_amount) AS total_revenue
        FROM fct_trips
        {where}
    """, params)

@st.cache_data
def get_daily(where, params):
    return run_query(f"""
        SELECT 
            date,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue
        FROM fct_trips
        {where}
        GROUP BY date
        ORDER BY date
    """, params)

@st.cache_data
def get_trips_per_shift(where, params):
    return run_query(f"""
        SELECT 
            shift,
            COUNT(*) AS total_trips
        FROM fct_trips
        {where}
        GROUP BY shift
        ORDER BY shift
    """, params)

@st.cache_data
def get_payments_types(where, params):
    return run_query(f"""
        SELECT 
            payment_type_name,
            COUNT(*) AS total_trips
        FROM fct_trips
        {where}
        GROUP BY payment_type_name
        ORDER BY payment_type_name
    """, params)

@st.cache_data
def get_heatmap_trips(where, params):
    return run_query(f"""
        SELECT  
            trip_hour,
            day_name,
            COUNT(*) AS total_trips
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
daily = get_daily(where, params)
trips_shift = get_trips_per_shift(where, params)
payments_types = get_payments_types(where, params)
heatmap = get_heatmap_trips(where, params)

# ===== UI =====
st.title("Dashboard NYC Taxi Trips")
st.subheader("Visão Geral")

row = kpis.iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total de Viagens", f"{row['total_trips']:,}")
col2.metric("Receita Total", f"${row['total_revenue']:,.2f}")
col3.metric("Ticket Médio", f"${row['avg_revenue']:,.2f}")
col4.metric("Distância Média", f"{row['avg_mile_distance']:.2f} milhas")
col5.metric("Duração Média", f"{row['avg_duration_min']:.1f} min")

col7, col8 = st.columns(2)

col7.subheader("Viagens por Turno")
col7.bar_chart(trips_shift.set_index("shift")["total_trips"], sort=True)

fig = px.pie(
    payments_types,
    names="payment_type_name",
    values="total_trips"
)
col8.subheader("Distribuição de Viagens por Tipo de Pagamento")
col8.plotly_chart(fig, use_container_width=True)

col9, col10 = st.columns(2)
col9.subheader("Viagens por Dia")
col9.line_chart(daily.set_index("date")["total_trips"])

col10.subheader("Receita por Dia")
col10.line_chart(daily.set_index("date")["total_revenue"])

fig = px.density_heatmap(
    heatmap,
    x="trip_hour",
    y="day_name",
    z="total_trips",
    color_continuous_scale="Blues",
    labels={
        "trip_hour": "Hora do Dia",
        "day_name": "Dia da Semana",
        "total_trips": "Total de Viagens"
    }
)

st.subheader("Heatmap de Viagens por Dia da Semana e Hora")
st.plotly_chart(fig, use_container_width=True)