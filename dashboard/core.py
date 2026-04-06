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

    return (
        shifts["shift"].tolist(),
        payments["payment_type_name"].tolist(),
        vendors["VendorID"].tolist()
    )

def init_filters():
    if "filters" not in st.session_state:
        st.session_state.filters = {
            "shift": "Todos",
            "payment": "Todos",
            "vendor": "Todos",
            "hour_range": (0, 23)
        }

def render_filters():
    shifts, payments, vendors = get_filters()

    st.sidebar.header("Filtros")

    filters = st.session_state.filters

    filters["hour_range"] = st.sidebar.slider(
        "Intervalo de Hora", 0, 23, filters["hour_range"]
    )

    filters["shift"] = st.sidebar.selectbox(
        "Turno", 
        ["Todos"] + shifts,
        format_func=lambda x: x.title() if x != "Todos" else x
    )

    filters["payment"] = st.sidebar.selectbox(
        "Pagamento", 
        ["Todos"] + payments,
        format_func=lambda x: x.title() if x != "Todos" else x
    )

    filters["vendor"] = st.sidebar.selectbox(
        "Fabricante", 
        ["Todos"] + vendors,
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

    start, end = filters["hour_range"]
    clauses.append("trip_hour BETWEEN ? AND ?")
    params.extend([start, end])

    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params