from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


DATA_DIR = Path(
    os.getenv("ICEBERG_DASHBOARD_DATA_DIR", Path(__file__).parent / "data")
)
TABLES = {
    "countries": "gold_sales_by_country",
    "months": "gold_sales_by_month",
    "products": "gold_top_products",
}

st.set_page_config(
    page_title="Iceberg Lakehouse | E-commerce",
    page_icon="❄️",
    layout="wide",
)


def load_export(table_name: str) -> pd.DataFrame:
    """Load all Spark CSV part files for one exported Gold table."""
    table_dir = DATA_DIR / table_name
    csv_files = sorted(table_dir.glob("part-*.csv"))
    if not csv_files:
        return pd.DataFrame()

    frames = [pd.read_csv(path) for path in csv_files]
    return pd.concat(frames, ignore_index=True)


def format_number(value: float | int) -> str:
    return f"{value:,.0f}".replace(",", " ")


st.title("❄️ Apache Iceberg Lakehouse")
st.caption(
    "Tableau de bord e-commerce · Spark transforme les données, "
    "Iceberg gère les tables, MinIO stocke les fichiers."
)

with st.sidebar:
    st.header("À propos des données")
    st.write("Tables Gold Iceberg exportées par le job Spark vers des fichiers CSV.")
    st.caption(
        "Pour actualiser le tableau de bord, relancez le job "
        "`04_silver_to_gold.py`."
    )
    if st.button("Actualiser les données", width="stretch"):
        st.rerun()

if not DATA_DIR.exists():
    st.error(f"Le dossier d'exports n'existe pas : `{DATA_DIR}`.")
    st.stop()

with st.spinner("Chargement des exports Gold…"):
    countries = load_export(TABLES["countries"])
    months = load_export(TABLES["months"])
    products = load_export(TABLES["products"])

missing_tables = [
    TABLES[key]
    for key, dataframe in (
        ("countries", countries),
        ("months", months),
        ("products", products),
    )
    if dataframe.empty
]

if missing_tables:
    st.warning(
        "Les exports Gold ne sont pas encore disponibles ou sont incomplets. "
        "Lancez `04_silver_to_gold.py`, puis actualisez cette page. "
        f"Tables manquantes : {', '.join(missing_tables)}."
    )
    st.stop()

required_columns = {
    "countries": {"Country", "total_quantity", "total_revenue", "invoice_count", "customer_count"},
    "months": {"year", "month", "total_quantity", "total_revenue", "invoice_count", "customer_count"},
    "products": {"StockCode", "Description", "total_quantity", "total_revenue", "invoice_count"},
}
for key, dataframe in (
    ("countries", countries),
    ("months", months),
    ("products", products),
):
    missing_columns = required_columns[key].difference(dataframe.columns)
    if missing_columns:
        st.error(
            f"Export `{TABLES[key]}` invalide : colonnes manquantes "
            f"{', '.join(sorted(missing_columns))}."
        )
        st.stop()

months["year"] = pd.to_numeric(months["year"], errors="coerce")
months["month"] = pd.to_numeric(months["month"], errors="coerce")
months["total_revenue"] = pd.to_numeric(months["total_revenue"], errors="coerce")
months["total_quantity"] = pd.to_numeric(months["total_quantity"], errors="coerce")
months["invoice_count"] = pd.to_numeric(months["invoice_count"], errors="coerce")
months["period"] = pd.to_datetime(
    dict(year=months["year"], month=months["month"], day=1),
    errors="coerce",
)
months = months.dropna(subset=["period"]).sort_values("period")

countries["total_revenue"] = pd.to_numeric(countries["total_revenue"], errors="coerce")
products["total_revenue"] = pd.to_numeric(products["total_revenue"], errors="coerce")

total_revenue = months["total_revenue"].sum()
total_invoices = months["invoice_count"].sum()

metric_columns = st.columns(4)
metric_columns[0].metric("Chiffre d’affaires", f"{total_revenue:,.2f} £")
metric_columns[1].metric("Factures", format_number(total_invoices))
metric_columns[2].metric("Pays", format_number(countries["Country"].nunique()))
metric_columns[3].metric("Produits", format_number(products["StockCode"].nunique()))

st.divider()

left_column, right_column = st.columns([1.15, 1])
with left_column:
    st.subheader("Évolution mensuelle du chiffre d’affaires")
    st.line_chart(
        months,
        x="period",
        y="total_revenue",
        alt="Évolution mensuelle du chiffre d’affaires",
    )

with right_column:
    st.subheader("Chiffre d’affaires par pays")
    country_options = ["Tous les pays"] + sorted(
        countries["Country"].dropna().astype(str).unique().tolist()
    )
    selected_country = st.selectbox("Filtrer par pays", country_options)
    country_view = countries.copy()
    if selected_country != "Tous les pays":
        country_view = country_view[country_view["Country"] == selected_country]
    st.bar_chart(
        country_view.sort_values("total_revenue", ascending=False),
        x="Country",
        y="total_revenue",
        alt="Chiffre d’affaires par pays",
    )

st.subheader("Meilleurs produits par chiffre d’affaires")
search = st.text_input("Rechercher un produit (code ou description)")
product_view = products.copy()
if search.strip():
    query = search.strip()
    matches = (
        product_view["StockCode"]
        .astype(str)
        .str.contains(query, case=False, na=False, regex=False)
        | product_view["Description"]
        .astype(str)
        .str.contains(query, case=False, na=False, regex=False)
    )
    product_view = product_view[matches]

product_view = product_view.sort_values("total_revenue", ascending=False).head(20)
st.dataframe(
    product_view,
    hide_index=True,
    alt="Top 20 produits triés par chiffre d’affaires",
    column_config={
        "total_revenue": st.column_config.NumberColumn("Chiffre d’affaires", format="%.2f £"),
        "total_quantity": st.column_config.NumberColumn("Quantité", format="%d"),
        "invoice_count": st.column_config.NumberColumn("Factures", format="%d"),
    },
)

export_timestamp = DATA_DIR / "_dashboard_exported_at.txt"
if export_timestamp.is_file():
    timestamp = export_timestamp.read_text(encoding="utf-8").strip()
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp)
        st.caption(f"Dernière génération des exports : {parsed_timestamp:%d/%m/%Y à %H:%M:%S UTC}")
    except ValueError:
        st.warning("La date de génération des exports est illisible.")
else:
    st.warning("La date de génération des exports n’est pas disponible.")
