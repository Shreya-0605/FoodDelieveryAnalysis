from pathlib import Path

import pandas as pd
import streamlit as st


OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_FILES = {
    "orders_by_date": OUTPUT_DIR / "orders_by_date.csv",
    "orders_by_hour": OUTPUT_DIR / "orders_by_hour.csv",
    "order_status": OUTPUT_DIR / "order_status.csv",
    "top_restaurants": OUTPUT_DIR / "top_restaurants.csv",
    "customer_summary": OUTPUT_DIR / "customer_summary.csv",
}

st.set_page_config(page_title="Food Delivery Analytics", layout="wide")
st.title("Food Delivery Analytics")
st.caption("A simple dashboard built from the PySpark analysis results.")

missing_files = [
    path.name for path in OUTPUT_FILES.values() if not path.is_file()
]
if missing_files:
    st.error(
        "Analysis output files are missing: "
        + ", ".join(missing_files)
        + ". Run the PySpark analysis first."
    )
    st.stop()

orders_by_date = pd.read_csv(OUTPUT_FILES["orders_by_date"])
orders_by_hour = pd.read_csv(OUTPUT_FILES["orders_by_hour"])
order_status = pd.read_csv(OUTPUT_FILES["order_status"])
top_restaurants = pd.read_csv(OUTPUT_FILES["top_restaurants"])
customer_summary = pd.read_csv(OUTPUT_FILES["customer_summary"])

status_options = sorted(
    order_status["order_status"].fillna("Unknown").astype(str).unique()
)
selected_status = st.selectbox("Order Status", ["All"] + status_options)


def filter_by_status(dataframe):
    if selected_status == "All":
        return dataframe

    statuses = dataframe["order_status"].fillna("Unknown").astype(str)
    return dataframe.loc[statuses == selected_status]


filtered_dates = filter_by_status(orders_by_date)
filtered_hours = filter_by_status(orders_by_hour)
filtered_restaurants = filter_by_status(top_restaurants)
filtered_status = filter_by_status(order_status)

summary_status = (
    "__ALL_ORDERS__" if selected_status == "All" else selected_status
)
summary_statuses = customer_summary["order_status"].fillna(
    "Unknown"
).astype(str)
selected_customer_summary = customer_summary.loc[
    summary_statuses == summary_status
]

if selected_customer_summary.empty:
    st.error("Customer summary is unavailable for the selected order status.")
    st.stop()

customer_metrics = selected_customer_summary.iloc[0]
total_orders = int(filtered_dates["total_orders"].sum())
unique_customers = int(customer_metrics["total_unique_customers"])
total_restaurants = int(
    filtered_restaurants["restaurant_id"].nunique(dropna=True)
)
repeat_customers = int(customer_metrics["repeat_customers"])

kpi_columns = st.columns(4)
kpi_columns[0].metric("Total Orders", f"{total_orders:,}")
kpi_columns[1].metric("Unique Customers", f"{unique_customers:,}")
kpi_columns[2].metric("Total Restaurants", f"{total_restaurants:,}")
kpi_columns[3].metric("Repeat Customers", f"{repeat_customers:,}")

st.subheader("Order Status Distribution")
status_chart = filtered_status.copy()
status_chart["order_status"] = status_chart["order_status"].fillna("Unknown")
st.bar_chart(status_chart.set_index("order_status")["total_orders"])

left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Orders by Date")
    date_chart = (
        filtered_dates.groupby("order_date", as_index=False)["total_orders"]
        .sum()
        .sort_values("order_date")
    )
    st.line_chart(date_chart.set_index("order_date")["total_orders"])

with right_column:
    st.subheader("Orders by Hour")
    hour_chart = (
        filtered_hours.groupby("order_hour", as_index=False)["total_orders"]
        .sum()
        .sort_values("order_hour")
    )
    st.bar_chart(hour_chart.set_index("order_hour")["total_orders"])

left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Top 10 Restaurants")
    restaurant_chart = filtered_restaurants.copy()
    restaurant_chart["restaurant_name"] = restaurant_chart[
        "restaurant_name"
    ].fillna("Unknown")
    restaurant_chart = (
        restaurant_chart.groupby("restaurant_name", as_index=False)[
            "total_orders"
        ]
        .sum()
        .sort_values("total_orders", ascending=False)
        .head(10)
    )
    st.bar_chart(
        restaurant_chart.set_index("restaurant_name")["total_orders"]
    )

with right_column:
    st.subheader("Repeat vs One-Time Customers")
    customer_chart = pd.DataFrame(
        {
            "Customer Type": ["Repeat Customers", "One-Time Customers"],
            "Customers": [
                int(customer_metrics["repeat_customers"]),
                int(customer_metrics["one_time_customers"]),
            ],
        }
    )
    st.bar_chart(customer_chart.set_index("Customer Type")["Customers"])
