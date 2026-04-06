import streamlit as st
import plotly.express as px
from core import init_filters, render_filters, build_where, run_query

st.set_page_config(layout="wide")

# ===== GLOBAL =====
init_filters()
render_filters()

filters = st.session_state.filters
where, params = build_where(filters)

# ===== DASHBOARD ANÁLISE DE PASSAGEIROS E DISTÂNCIA =====
@st.cache_data
def get_kpis(where, params):
    return run_query(f"""
        SELECT 
            ROUND(AVG(passenger_count), 2) AS avg_passenger,
            AVG(trip_distance) AS avg_mile_distance,
            AVG(trip_duration_min) AS avg_duration_min
        FROM fct_trips
        {where}
    """, params)

@st.cache_data
def get_kpis_passenger(where, params):
    return run_query(f"""
        SELECT  
            passenger_count,
            COUNT(*) AS total_trips
        FROM fct_trips
        {where}
        GROUP BY
            passenger_count
        ORDER BY
            passenger_count
    """, params)

@st.cache_data
def get_scatter_data(where, params):
    return run_query(f"""
        SELECT
            trip_distance,
            trip_duration_min,
            passenger_count,
            total_amount,
            payment_type_name
        FROM fct_trips
        {where}
        LIMIT 5000
    """, params)

kpis = get_kpis(where, params)
kpis_passenger = get_kpis_passenger(where, params)
scatter = get_scatter_data(where, params).sample(n=5000, random_state=42)

# ===== UI =====
st.title("Dashboard NYC Taxi Trips")
st.subheader("Análise de Passageiros e Distância")

row = kpis.iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric("Média de Passageiros", f"{row['avg_passenger']:,}")
col2.metric("Distância Média", f"{row['avg_mile_distance']:.2f} milhas")
col3.metric("Duração Média", f"{row['avg_duration_min']:.1f} min")

st.subheader("Passageiros por Corrida")
st.bar_chart(kpis_passenger.set_index("passenger_count")["total_trips"])

fig = px.scatter(
    scatter,
    x="trip_distance",
    y="trip_duration_min",
    size="passenger_count",
    color="payment_type_name",
    hover_data=["total_amount"],
    labels={
        "trip_distance": "Distância (milhas)",
        "trip_duration_min": "Duração (min)",
        "passenger_count": "Qtd. Passageiros",
        "payment_type_name": "Tipo de Pagamento"
    }
)

st.subheader("Dispersão de Duração (min) por Distância (milhas)")
st.plotly_chart(fig, use_container_width=True)