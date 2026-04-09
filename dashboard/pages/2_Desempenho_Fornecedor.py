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
            VendorID,
            trip_hour,
            AVG(speed_mph) AS avg_speed
        FROM 
            fct_trips
        {where}
        GROUP BY 
            VendorID, trip_hour
    """, params)

@st.cache_data
def get_distance_mix(where, params):
    return run_query(f"""
        SELECT
            VendorID,
            distance_category,
            COUNT(*) AS total_trips
        FROM 
            fct_trips
        {where}
        GROUP BY 
            VendorID, distance_category
    """, params)


@st.cache_data
def get_payment_mix(where, params):
    return run_query(f"""
        SELECT
            VendorID,
            payment_type_name,
            COUNT(*) AS total_trips
        FROM 
            fct_trips
        {where}
        GROUP BY 
            VendorID, payment_type_name
    """, params)

kpis = get_kpis(where, params)
kpis_vendor = get_kpis_vendor(where, params)
heatmap = get_heatmap_speed(where, params)
distance_mix = get_distance_mix(where, params)
payment_mix = get_payment_mix(where, params)

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

col8, col9 = st.columns(2)
fig_dist = px.bar(
    distance_mix,
    x="VendorID",
    y="total_trips",
    color="distance_category",
    barmode="relative",
    category_orders={
        "distance_category": ["Curta", "Média", "Longa"]
    },
    labels={
        "VendorID": "Fornecedor",
        "total_trips": "Total de Viagens",
        "distance_category": "Distância"
    }
)

col8.subheader("Viagens por Fornecedor e Distância")
col8.plotly_chart(fig_dist, use_container_width=True)

fig_pay = px.bar(
    payment_mix,
    x="VendorID",
    y="total_trips",
    color="payment_type_name",
    barmode="relative",
    labels={
        "VendorID": "Fornecedor",
        "total_trips": "Total de Viagens",
        "payment_type_name": "Tipo de Pagamento"
    }
)

col9.subheader("Distribuição de Pagamentos por Fornecedor")
col9.plotly_chart(fig_pay, use_container_width=True)

fig = px.density_heatmap(
    heatmap,
    x="trip_hour",
    y="VendorID",
    z="avg_speed",
    color_continuous_scale="Blues",
    labels={
        "trip_hour": "Hora do Dia",
        "VendorID": "Fornecedor",
        "avg_speed": "Média de Velocidade (mph)"
    }
)

st.subheader("Heatmap de Velocidade (mph) por Fornecedor")
st.plotly_chart(fig, use_container_width=True)
