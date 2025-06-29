import pandas as pd
from collections import Counter

def generate_metrics(df: pd.DataFrame, groupby: str) -> dict:
    # Convert date column to datetime
    df["order_date"] = pd.to_datetime(df["purchased_date"])

    # Determine grouping key
    if groupby == "month":
        df["period"] = df["order_date"].dt.to_period("M").astype(str)
    elif groupby == "year":
        df["period"] = df["order_date"].dt.to_period("Y").astype(str)
    else:
        raise ValueError("Invalid groupby value")

    # Global metrics
    grand_totals = {
        "total_orders": int(len(df)),
        "gross_sales": float(df["gross_sales"].sum()),
        "net_sales": float(df["net_sales"].sum()),
        "grand_total": float(df["grand_total"].sum()),
        "most_popular_product_sku": df["sku"].mode().iloc[0],
        "least_popular_product_sku": df["sku"].value_counts().idxmin()
    }

    # Grouped metrics
    grouped = df.groupby("period").agg(
        total_orders=("order_id", "count"),
        gross_sales=("gross_sales", "sum"),
        net_sales=("net_sales", "sum"),
        grand_total=("grand_total", "sum")
    ).reset_index()

    result = []
    for _, row in grouped.iterrows():
        period_df = df[df["period"] == row["period"]]
        sku_counts = period_df["sku"].value_counts()
        result.append({
            "period": row["period"],
            "total_orders": int(row["total_orders"]),
            "gross_sales": float(row["gross_sales"]),
            "net_sales": float(row["net_sales"]),
            "grand_total": float(row["grand_total"]),
            "most_popular_product_sku": sku_counts.idxmax(),
            "least_popular_product_sku": sku_counts.idxmin()
        })

    return grand_totals, result
