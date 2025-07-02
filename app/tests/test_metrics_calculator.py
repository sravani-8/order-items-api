import pytest
import pandas as pd
from app.services.metrics_calculator import generate_metrics, sanitize_columns

# Fixture for a sample DataFrame
@pytest.fixture
def sample_dataframe():
    data = {
        'order_id': ['ord1', 'ord2', 'ord3', 'ord4', 'ord5', 'ord6', 'ord7', 'ord8', 'ord9', 'ord10'],
        'sku': ['SKU001', 'SKU002', 'SKU001', 'SKU003', 'SKU002', 'SKU004', 'SKU001', 'SKU005', 'SKU003', 'SKU006'],
        'item_price': [10.0, 20.0, 15.0, 5.0, 25.0, 30.0, 12.0, 18.0, 22.0, 8.0],
        'item_tax': [1.0, 2.0, 1.5, 0.5, 2.5, 3.0, 1.2, 1.8, 2.2, 0.8],
        'item_discount': [0.5, 1.0, 0.75, 0.25, 1.25, 1.5, 0.6, 0.9, 1.1, 0.4],
        'purchased_date': [
            '2024-01-15', '2024-01-20', '2024-02-01', '2024-02-10', '2024-03-05',
            '2024-03-10', '2024-04-01', '2024-04-10', '2024-05-01', '2024-05-15'
        ]
    }
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def empty_dataframe():
    return pd.DataFrame(columns=['order_id', 'sku', 'item_price', 'item_tax', 'item_discount', 'purchased_date'])

# Test sanitize_columns
def test_sanitize_columns():
    cols = ["Order ID ", "Product SKU", "Item-Price", "Purchase Date(YYYY-MM-DD)", "My@Column#Name"]
    sanitized = sanitize_columns(cols)
    assert sanitized == ["order_id", "product_sku", "item_price", "purchase_date_yyyy_mm_dd", "my_column_name"]

def test_sanitize_columns_empty_and_duplicate_names():
    cols = ["col1", "", "col1", "col2"]
    sanitized = sanitize_columns(cols)
    assert sanitized == ["col1", "unnamed_0", "col1_0", "col2"]

# Test generate_metrics
def test_generate_metrics_groupby_month(sample_dataframe):
    grand_totals, metrics, start_date, end_date = generate_metrics(sample_dataframe.copy(), "month")

    assert start_date == "2024-01-15"
    assert end_date == "2024-05-15"
    assert grand_totals["total_orders"] == 10
    assert abs(grand_totals["gross_sales"] - 195.0) < 0.01
    assert abs(grand_totals["net_sales"] - 21.0) < 0.01 # Sum of item_tax, not item_price + item_tax
    assert abs(grand_totals["grand_total"] - 200.6) < 0.01

    # Recalculate net_sales and grand_total based on the logic in metrics_calculator
    # df["net_sales"] = df["gross_sales"] + df["tax_total"]
    # df["grand_total"] = df["net_sales"] - df["discount_total"]
    expected_gross_sales = sample_dataframe['item_price'].sum()
    expected_tax_total = sample_dataframe['item_tax'].sum()
    expected_discount_total = sample_dataframe['item_discount'].sum()
    expected_net_sales = expected_gross_sales + expected_tax_total
    expected_grand_total = expected_net_sales - expected_discount_total

    assert abs(grand_totals["gross_sales"] - expected_gross_sales) < 0.01
    assert abs(grand_totals["net_sales"] - expected_net_sales) < 0.01
    assert abs(grand_totals["grand_total"] - expected_grand_total) < 0.01

    assert grand_totals["most_popular_product_sku"] == "SKU001"
    assert grand_totals["least_popular_product_sku"] == "SKU006" # Or SKU004, SKU005 if counts are 1

    assert len(metrics) == 5 # Jan, Feb, Mar, Apr, May
    
    # Verify a specific month's metrics (e.g., January)
    jan_metrics = next(m for m in metrics if m["period"] == "2024-01")
    assert jan_metrics["total_orders"] == 2
    assert abs(jan_metrics["gross_sales"] - 30.0) < 0.01 # 10 + 20
    assert abs(jan_metrics["net_sales"] - 33.0) < 0.01  # 30 + 1 + 2 = 33
    assert abs(jan_metrics["grand_total"] - 31.5) < 0.01 # 33 - (0.5+1.0) = 31.5
    assert jan_metrics["most_popular_product_sku"] == "SKU001"
    assert jan_metrics["least_popular_product_sku"] == "SKU002"

def test_generate_metrics_groupby_year(sample_dataframe):
    grand_totals, metrics, start_date, end_date = generate_metrics(sample_dataframe.copy(), "year")

    assert start_date == "2024-01-15"
    assert end_date == "2024-05-15"
    assert grand_totals["total_orders"] == 10
    # Calculations based on the whole dataset (same as above)
    expected_gross_sales = sample_dataframe['item_price'].sum()
    expected_tax_total = sample_dataframe['item_tax'].sum()
    expected_discount_total = sample_dataframe['item_discount'].sum()
    expected_net_sales = expected_gross_sales + expected_tax_total
    expected_grand_total = expected_net_sales - expected_discount_total
    
    assert abs(grand_totals["gross_sales"] - expected_gross_sales) < 0.01
    assert abs(grand_totals["net_sales"] - expected_net_sales) < 0.01
    assert abs(grand_totals["grand_total"] - expected_grand_total) < 0.01

    assert len(metrics) == 1 # Only one year (2024)
    assert metrics[0]["period"] == "2024"
    assert metrics[0]["total_orders"] == 10
    assert abs(metrics[0]["gross_sales"] - expected_gross_sales) < 0.01
    assert abs(metrics[0]["net_sales"] - expected_net_sales) < 0.01
    assert abs(metrics[0]["grand_total"] - expected_grand_total) < 0.01
    assert metrics[0]["most_popular_product_sku"] == "SKU001"
    assert metrics[0]["least_popular_product_sku"] == "SKU006"

def test_generate_metrics_missing_purchased_date(sample_dataframe):
    df_no_date = sample_dataframe.drop(columns=['purchased_date'])
    with pytest.raises(ValueError, match="Missing required column 'purchased_date'"):
        generate_metrics(df_no_date, "month")

def test_generate_metrics_invalid_groupby(sample_dataframe):
    with pytest.raises(ValueError, match="Invalid groupby value"):
        generate_metrics(sample_dataframe, "quarter")

def test_generate_metrics_empty_dataframe(empty_dataframe):
    grand_totals, metrics, start_date, end_date = generate_metrics(empty_dataframe, "month")
    
    assert grand_totals["total_orders"] == 0
    assert grand_totals["gross_sales"] == 0.0
    assert grand_totals["net_sales"] == 0.0
    assert grand_totals["grand_total"] == 0.0
    assert grand_totals["most_popular_product_sku"] is None
    assert grand_totals["least_popular_product_sku"] is None
    assert metrics == []
    assert start_date is None
    assert end_date is None

def test_generate_metrics_with_non_numeric_prices(sample_dataframe):
    df_mixed_prices = sample_dataframe.copy()
    df_mixed_prices.loc[0, 'item_price'] = 'abc'
    df_mixed_prices.loc[1, 'item_tax'] = 'xyz'
    
    grand_totals, metrics, start_date, end_date = generate_metrics(df_mixed_prices, "month")

    # The 'abc' and 'xyz' should be coerced to 0.0
    expected_gross_sales_after_coerce = sample_dataframe['item_price'].iloc[1:].sum()
    expected_tax_total_after_coerce = sample_dataframe['item_tax'].iloc[0:1].sum() + sample_dataframe['item_tax'].iloc[2:].sum()

    expected_gross_sales = sample_dataframe['item_price'].sum() # original sum, then abc becomes 0
    expected_tax_total = sample_dataframe['item_tax'].sum() # original sum, then xyz becomes 0
    expected_discount_total = sample_dataframe['item_discount'].sum()

    # Recalculate sums considering 'abc' and 'xyz' become 0.0
    expected_gross_sales_calculated = (sample_dataframe['item_price'].drop(0).sum()) + 0.0 # 'abc' becomes 0
    expected_tax_total_calculated = (sample_dataframe['item_tax'].drop(1).sum()) + 0.0 # 'xyz' becomes 0

    expected_net_sales_calculated = expected_gross_sales_calculated + expected_tax_total_calculated
    expected_grand_total_calculated = expected_net_sales_calculated - expected_discount_total

    assert abs(grand_totals["gross_sales"] - expected_gross_sales_calculated) < 0.01
    assert abs(grand_totals["net_sales"] - expected_net_sales_calculated) < 0.01
    assert abs(grand_totals["grand_total"] - expected_grand_total_calculated) < 0.01