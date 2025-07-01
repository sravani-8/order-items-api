import pandas as pd
import unicodedata
import re
from collections import Counter

def sanitize_columns(columns):
    def clean(name):
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_bytes = nfkd.encode("ascii", "ignore")
        text = ascii_bytes.decode("ascii")
        text = re.sub(r"[^\w]", "_", text)
        text = re.sub(r"_+", "_", text)
        return text.strip("_").lower()
    return [clean(col) for col in columns]

def generate_metrics(df: pd.DataFrame, groupby: str):
    # 1) Sanitize headers
    df.columns = sanitize_columns(df.columns.tolist())

    # 2) Parse date
    if "purchased_date" not in df.columns:
        raise ValueError("Missing required column 'purchased_date'")
    df["order_date"] = pd.to_datetime(df["purchased_date"], errors="coerce")
    df = df.dropna(subset=["order_date"])

    # 3) Compute sales columns
    # Identify columns
    price_cols    = [c for c in df.columns if "price" in c]
    tax_cols      = [c for c in df.columns if "tax" in c]
    discount_cols = [c for c in df.columns if "discount" in c]

    # Coerce numeric
    df[price_cols]    = df[price_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    df[tax_cols]      = df[tax_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    df[discount_cols] = df[discount_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    # Calculate
    df["gross_sales"]   = df[price_cols].sum(axis=1)
    df["tax_total"]     = df[tax_cols].sum(axis=1)
    df["discount_total"]= df[discount_cols].sum(axis=1)
    df["net_sales"]     = df["gross_sales"] + df["tax_total"]
    df["grand_total"]   = df["net_sales"] - df["discount_total"]

    # 4) Grouping key
    if groupby == "month":
        df["period"] = df["order_date"].dt.to_period("M").astype(str)
    elif groupby == "year":
        df["period"] = df["order_date"].dt.to_period("Y").astype(str)
    else:
        raise ValueError("Invalid groupby value")

    # 5) Grand totals
    grand_totals = {
        #"total_orders":         int(len(df)),
         "total_orders": int(df.shape[0]),
        "gross_sales":          float(df["gross_sales"].sum()),
        "net_sales":            float(df["net_sales"].sum()),
        "grand_total":          float(df["grand_total"].sum()),
       # "most_popular_product_sku": df["sku"].mode().iloc[0] if "sku" in df.columns else None,
       # "least_popular_product_sku": df["sku"].value_counts().idxmin() if "sku" in df.columns else None
      "most_popular_product_sku": (
       df["sku"].mode().iloc[0] if "sku" in df.columns and not df["sku"].mode().empty else None),
        "least_popular_product_sku": (
            df["sku"].value_counts().idxmin() if "sku" in df.columns and not df["sku"].value_counts().empty else None
        )

    }

    # 6) Per-period metrics
   # grouped = df.groupby("period").agg(
       # total_orders=("order_id", "count"),
       # gross_sales=("gross_sales", "sum"),
        #net_sales=("net_sales", "sum"),
        #grand_total=("grand_total", "sum")
    #).reset_index()

    # 6) Per-period metrics
    grp = df.groupby("period")
    metrics_df = grp.agg(
        gross_sales=("gross_sales", "sum"),
        net_sales   =("net_sales",   "sum"),
        grand_total =("grand_total", "sum")
    )
    metrics_df["total_orders"] = grp.size()
    metrics_df = metrics_df.reset_index() 

    metrics = []
   # for _, row in grouped.iterrows():
    for _, row in metrics_df.iterrows():
        period_df = df[df["period"] == row["period"]]
        sku_counts = period_df["sku"].value_counts() if "sku" in df.columns else Counter()
        metrics.append({
            "period": row["period"],
            "total_orders":      int(row["total_orders"]),
            "gross_sales":       float(row["gross_sales"]),
            "net_sales":         float(row["net_sales"]),
            "grand_total":       float(row["grand_total"]),
            "most_popular_product_sku": sku_counts.idxmax() if len(sku_counts) else None,
            "least_popular_product_sku": sku_counts.idxmin() if len(sku_counts) else None
        })

    start_date = df["order_date"].min().date().isoformat()
    end_date   = df["order_date"].max().date().isoformat()
    return grand_totals, metrics, start_date, end_date
