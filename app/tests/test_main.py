from datetime import datetime
from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient
import requests
from app.services.file_handler import file_storage
from main import app # Import your FastAPI app instance

# Create a TestClient instance
client = TestClient(app)

# Fixture to clear file_storage before each test
@pytest.fixture(autouse=True)
def clear_file_storage():
    file_storage.clear()
    yield # Let the test run
    file_storage.clear() # Clear again after the test

# Mock CSV content for testing
MOCK_CSV_CONTENT_VALID = """order_id,sku,item_price,item_tax,purchased_date,order_item_id
1,SKU001,10.0,1.0,2024-01-01,item1
2,SKU002,20.0,2.0,2024-01-02,item2
3,SKU001,15.0,1.5,2024-02-01,item3
1,SKU001,10.0,1.0,2024-01-01,item1 # Duplicate
"""

MOCK_CSV_CONTENT_INVALID_DATA = """order_id,sku,item_price,item_tax,purchased_date
1,SKU001,invalid_price,1.0,2024-01-01
2,SKU002,20.0,bad_tax,2024-01-02
"""

MOCK_CSV_URL = "http://example.com/test_valid.csv"
MOCK_CSV_URL_INVALID = "http://example.com/test_invalid.csv"
MOCK_CSV_URL_404 = "http://example.com/not_found.csv"


# Mock requests.get for file_handler's download_and_clean_csv
@pytest.fixture(autouse=True)
def mock_requests_get(mocker):
    def mock_get_side_effect(url, *args, **kwargs):
        if url == MOCK_CSV_URL:
            # MockResponse expects content as a string, then encodes it
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = MOCK_CSV_CONTENT_VALID.encode('utf-8')
            mock_response.raise_for_status.return_value = None
            return mock_response
        elif url == MOCK_CSV_URL_INVALID:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = MOCK_CSV_CONTENT_INVALID_DATA.encode('utf-8')
            mock_response.raise_for_status.return_value = None
            return mock_response
        elif url == MOCK_CSV_URL_404:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
            return mock_response
        else:
            # Fallback for unexpected URLs, can raise an error or return default
            raise ValueError(f"Unexpected URL: {url}")

    mocker.patch('requests.get', side_effect=mock_get_side_effect)
    # Ensure datetime.utcnow() is consistent for testing uploaded_at
    fixed_time = datetime(2025, 7, 2, 0, 0, 0, 0)
    mocker.patch('datetime.utcnow', return_value=fixed_time)


# --- Tests for /upload endpoint ---
def test_upload_csv_url_success():
    response = client.post("/upload", data={"csv_url": MOCK_CSV_URL})
    assert response.status_code == 200
    assert "file_id" in response.json()
    assert response.json()["message"] == "File processed successfully"
    # Verify file_id is stored
    assert response.json()["file_id"] in file_storage

def test_upload_csv_url_invalid_url():
    response = client.post("/upload", data={"csv_url": MOCK_CSV_URL_404})
    assert response.status_code == 400
    assert "detail" in response.json()
    assert "Error downloading file" in response.json()["detail"]

def test_upload_csv_url_empty_url():
    response = client.post("/upload", data={"csv_url": ""})
    # FastAPI's Form(...) validation should catch this as required
    assert response.status_code == 422 # Unprocessable Entity due to validation error

# --- Tests for /api/v1/order-items/uploads/{file_id}/processing-stats endpoint ---
def test_get_processing_stats_success():
    # First, upload a file to get a file_id
    upload_response = client.post("/upload", data={"csv_url": MOCK_CSV_URL})
    file_id = upload_response.json()["file_id"]

    response = client.get(f"/api/v1/order-items/uploads/{file_id}/processing-stats")
    assert response.status_code == 200
    stats = response.json()
    assert "rows" in stats
    assert stats["rows"]["total"] == 4 # Header + 4 data rows
    assert stats["rows"]["blank"] == 0
    assert stats["rows"]["malformed"] == 0
    assert stats["rows"]["duplicated"] == 1 # 'item1' is duplicated
    assert stats["rows"]["sanitised"] == 4
    assert stats["rows"]["valid"] == 4
    assert stats["rows"]["usable"] == 3 # 4 sanitised - 1 duplicated
    assert stats["outcome"]["accepted"] == 3
    assert stats["outcome"]["rejected"] == 1 # 1 duplicated
    assert stats["uploaded_at"].startswith("2025-07-02T00:00:00") # Based on mocked datetime

def test_get_processing_stats_invalid_file_id_format():
    response = client.get("/api/v1/order-items/uploads/invalid_id/processing-stats")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid file ID format."

def test_get_processing_stats_non_existent_file_id():
    response = client.get("/api/v1/order-items/uploads/nonexistent-id-1234567890abcdef/processing-stats")
    assert response.status_code == 404
    assert response.json()["detail"] == "File ID does not exist."

# --- Tests for /api/v1/order-items/uploads/{file_id}/metrics endpoint ---
def test_get_metrics_groupby_month_success():
    # First, upload a file
    upload_response = client.post("/upload", data={"csv_url": MOCK_CSV_URL})
    file_id = upload_response.json()["file_id"]

    response = client.get(f"/api/v1/order-items/uploads/{file_id}/metrics?groupby=month")
    assert response.status_code == 200
    metrics_data = response.json()

    assert metrics_data["group_by"] == "month"
    assert metrics_data["grand_totals"]["total_orders"] == 3 # Usable rows after de-duplication
    assert len(metrics_data["metrics"]) == 2 # Jan, Feb periods
    
    jan_metrics = next(m for m in metrics_data["metrics"] if m["period"] == "2024-01")
    assert jan_metrics["total_orders"] == 2 # Two unique orders in Jan
    assert abs(jan_metrics["gross_sales"] - 30.0) < 0.01 # 10 + 20
    assert abs(jan_metrics["net_sales"] - 33.0) < 0.01 # 30 + 1 + 2
    assert abs(jan_metrics["grand_total"] - 33.0) < 0.01 # no discount in mock data
    
    assert metrics_data["grand_totals"]["total_orders"] == 3 # 3 unique order items after deduplication
    assert abs(metrics_data["grand_totals"]["gross_sales"] - (10+20+15)) < 0.01 # Sum of item prices (original - duplicates)
    assert abs(metrics_data["grand_totals"]["net_sales"] - ((10+20+15)+(1+2+1.5))) < 0.01 # Gross + Tax
    
def test_get_metrics_groupby_year_success():
    # First, upload a file
    upload_response = client.post("/upload", data={"csv_url": MOCK_CSV_URL})
    file_id = upload_response.json()["file_id"]

    response = client.get(f"/api/v1/order-items/uploads/{file_id}/metrics?groupby=year")
    assert response.status_code == 200
    metrics_data = response.json()

    assert metrics_data["group_by"] == "year"
    assert metrics_data["grand_totals"]["total_orders"] == 3
    assert len(metrics_data["metrics"]) == 1 # Only one year (2024)

def test_get_metrics_invalid_groupby():
    # First, upload a file
    upload_response = client.post("/upload", data={"csv_url": MOCK_CSV_URL})
    file_id = upload_response.json()["file_id"]

    response = client.get(f"/api/v1/order-items/uploads/{file_id}/metrics?groupby=invalid")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid groupby value"

def test_get_metrics_non_existent_file_id():
    response = client.get("/api/v1/order-items/uploads/nonexistent-id-1234567890abcdef/metrics?groupby=month")
    assert response.status_code == 404
    assert response.json()["detail"] == "File ID does not exist."

def test_get_metrics_invalid_file_id_format():
    response = client.get("/api/v1/order-items/uploads/short/metrics?groupby=month")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid file ID format."

def test_get_metrics_on_file_with_malformed_content():
    # Test that generate_metrics still runs on the DF, even if it had content malformed rows
    upload_response = client.post("/upload", data={"csv_url": MOCK_CSV_URL_INVALID})
    file_id = upload_response.json()["file_id"]

    # The processing-stats for this should show malformed rows
    stats_response = client.get(f"/api/v1/order-items/uploads/{file_id}/processing-stats")
    assert stats_response.status_code == 200
    assert stats_response.json()["rows"]["malformed"] == 2 # Based on MOCK_CSV_CONTENT_INVALID_DATA

    # Now request metrics; it should handle NaN values from coercion
    metrics_response = client.get(f"/api/v1/order-items/uploads/{file_id}/metrics?groupby=month")
    assert metrics_response.status_code == 200
    metrics_data = metrics_response.json()
    # Expecting 2 total orders since there are 2 rows, but sums should be 0 due to NaN coercion
    assert metrics_data["grand_totals"]["total_orders"] == 2
    assert abs(metrics_data["grand_totals"]["gross_sales"] - 0.0) < 0.01
    assert abs(metrics_data["grand_totals"]["net_sales"] - 0.0) < 0.01
    assert abs(metrics_data["grand_totals"]["grand_total"] - 0.0) < 0.01
    assert metrics_data["metrics"][0]["period"] == "2024-01"
    assert metrics_data["metrics"][0]["total_orders"] == 2
    assert abs(metrics_data["metrics"][0]["gross_sales"] - 0.0) < 0.01