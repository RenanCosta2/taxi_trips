import duckdb
import streamlit as st
from pathlib import Path

st.set_page_config(layout="wide")

path = Path("taxi_trips_dbt/dev.duckdb")

# ================== DATA ==================
@st.cache_data
def run_query(query, params=None):
    con = duckdb.connect(path)
    return con.execute(query, params or []).fetchdf()

@st.cache_data
def get_filters():
    shifts = run_query(
        """
            SELECT DISTINCT
                shift
            FROM 
                fct_trips
        """
    )

    payments = run_query(
        """
            SELECT DISTINCT
                payment_type_name
            FROM 
                fct_trips
        """
    )

    vendors = run_query(
        """
            SELECT DISTINCT
                VendorID
            FROM 
                fct_trips
            ORDER BY 
                VendorID ASC
        """
    )
    
    return (
            shifts["shift"].tolist(), 
            payments["payment_type_name"].tolist(), 
            vendors["VendorID"].tolist()
        )

# ================== FILTERS ==================
def render_filters():
    shifts, payments, vendors = get_filters()

    st.sidebar.header("Filtros")

    hour_range = st.sidebar.slider("Intervalo de Hora", 0, 23, (0, 23))

    return {
        "shift": st.sidebar.selectbox(
            "Turno", 
            ["Todos"] + shifts,
            format_func=lambda x: x.title() if x != "Todos" else x
        ),
        "payment": st.sidebar.selectbox(
            "Pagamento", 
            ["Todos"] + payments,
            format_func=lambda x: x.title() if x != "Todos" else x
        ),
        "vendor": st.sidebar.selectbox(
            "Fabricante", 
            ["Todos"] + vendors
        ),
        "hour_range": hour_range
    }

def build_where(filters):
    clauses = []
    params = []

    if filters["shift"] != "Todos":
        clauses.append("shift = ?")
        params.append(filters["shift"])

    if filters["payment"] != "Todos":
        clauses.append("payment_type_name = ?")
        params.append(filters["payment"])

    if filters["vendor"] != "Todos":
        clauses.append("VendorID = ?")
        params.append(filters["vendor"])

    start_hour, end_hour = filters["hour_range"]
    clauses.append("trip_hour BETWEEN ? AND ?")
    params.extend([start_hour, end_hour])

    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params

# ================== QUERIES ==================
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

# ================== UI ==================
def render_kpis(df):
    row = df.iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Total de Corridas", f"{row['total_trips']:,}")
    col2.metric("Receita Total", f"${row['total_revenue']:,.2f}")
    col3.metric("Ticket Médio", f"${row['avg_revenue']:,.2f}")
    col4.metric("Distância Média", f"{row['avg_mile_distance']:.2f}")
    col5.metric("Duração Média", f"{row['avg_duration_min']:.1f}")

def render_chart(df):
    st.subheader("Evolução diária")
    st.line_chart(df.set_index("date")["total_trips"])

# ================== MAIN ==================
filters = render_filters()
where, params = build_where(filters)

kpis = get_kpis(where, params)
daily = get_daily(where, params)

st.title("Dashboard Taxi")

st.subheader("Visão Geral")
render_kpis(kpis)

st.divider()

render_chart(daily)