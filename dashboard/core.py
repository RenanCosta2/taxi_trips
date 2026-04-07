import duckdb
import streamlit as st
from pathlib import Path

path = Path("taxi_trips_dbt/dev.duckdb")

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
                VendorID
        """
    )
    days_of_week = run_query(
        """
            SELECT DISTINCT 
                day_name 
            FROM 
                fct_trips 
            ORDER BY 
                day_of_week
        """
    )

    return (
        shifts["shift"].tolist(),
        payments["payment_type_name"].tolist(),
        vendors["VendorID"].tolist(),
        days_of_week["day_name"].tolist()
    )

def init_filters():
    if "filters" not in st.session_state:
        st.session_state.filters = {
            "shift": "Todos",
            "payment": "Todos",
            "vendor": "Todos",
            "hour_range": (0, 23),
            "days": []
        }

def render_filters():
    shifts, payments, vendors, days = get_filters()

    st.sidebar.header("Filtros")

    # inicialização segura
    if "shift" not in st.session_state:
        st.session_state.shift = "Todos"
    if "payment" not in st.session_state:
        st.session_state.payment = "Todos"
    if "vendor" not in st.session_state:
        st.session_state.vendor = "Todos"
    if "hour_range" not in st.session_state:
        st.session_state.hour_range = (0, 23)
    if "days" not in st.session_state:
        st.session_state.days = []

    # widgets com key (ESSENCIAL)
    st.sidebar.slider(
        "Intervalo de Hora",
        0, 23,
        key="hour_range"
    )

    st.sidebar.selectbox(
        "Turno",
        ["Todos"] + shifts,
        key="shift",
        format_func=lambda x: x.title() if x != "Todos" else x
    )

    st.sidebar.selectbox(
        "Pagamento",
        ["Todos"] + payments,
        key="payment",
        format_func=lambda x: x.title() if x != "Todos" else x
    )

    st.sidebar.selectbox(
        "Fabricante",
        ["Todos"] + vendors,
        key="vendor"
    )

    st.sidebar.multiselect(
        "Dia da Semana",
        options=days,
        key="days"
    )

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

    if filters["days"]:
        placeholders = ",".join(["?"] * len(filters["days"]))
        clauses.append(f"day_name IN ({placeholders})")
        params.extend(filters["days"])

    start, end = filters["hour_range"]
    clauses.append("trip_hour BETWEEN ? AND ?")
    params.extend([start, end])

    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params