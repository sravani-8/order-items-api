import pytest
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
from unittest.mock import MagicMock
from app.services.file_handler import download_and_clean_csv, _perform_detailed_analysis, file_storage

# Mock responses for requests.get
class MockResponse:
    def __init__(self, content, status_code=200, encoding='utf-8'):
        self._content = content.encode(encoding)
        self.status_code = status_code
        self.encoding = encoding

    @property
    def content(self):
        return self._content

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.exceptions.HTTPError(f"HTTP Error {self.status_code}")

# Helper function to reset file_storage between tests if needed (optional for unit tests)
@pytest.fixture(autouse=True)
def clean_file_storage():
    file_storage.clear()
    yield # Let test run
    file_storage.clear() # Clear again after test

# --- Tests for _perform_detailed_analysis function ---
def test_detailed_analysis_empty_csv():
    raw_text = ""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 0
    assert summary["rows"]["usable"] == 0
    assert df.empty

def test_detailed_analysis_only_header():
    raw_text = "col1,col2,col3"
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 0
    assert summary["rows"]["sanitised"] == 0
    assert df.empty
    assert list(df.columns) == ["col1", "col2", "col3"]

def test_detailed_analysis_valid_data():
    raw_text = """order_id,sku,item_price,item_tax,colX
1,A1,10.0,1.0,data1
2,B2,20.0,2.0,data2
3,C3,30.0,3.0,data3"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 3
    assert summary["rows"]["blank"] == 0
    assert summary["rows"]["malformed"] == 0
    assert summary["rows"]["duplicated"] == 0
    assert summary["rows"]["sanitised"] == 3
    assert summary["rows"]["valid"] == 3
    assert summary["rows"]["usable"] == 3
    assert summary["outcome"]["accepted"] == 3
    assert summary["outcome"]["rejected"] == 0
    assert len(df) == 3
    assert "order_id" in df.columns

def test_detailed_analysis_with_blank_rows():
    raw_text = """order_id,sku,item_price,item_tax
1,A1,10.0,1.0
,,,-
2,B2,20.0,2.0
,,,
3,C3,30.0,3.0"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 5
    assert summary["rows"]["blank"] == 2 # The two empty lines
    assert summary["rows"]["malformed"] == 0
    assert summary["rows"]["duplicated"] == 0
    assert summary["rows"]["sanitised"] == 3
    assert summary["rows"]["valid"] == 3
    assert summary["rows"]["usable"] == 3
    assert summary["outcome"]["accepted"] == 3
    assert summary["outcome"]["rejected"] == 2
    assert len(df) == 3

def test_detailed_analysis_with_structural_malformed_rows():
    # Rows with incorrect number of columns
    raw_text = """order_id,sku,item_price,item_tax
1,A1,10.0,1.0
2,B2,20.0
3,C3,30.0,3.0,extra
4,D4,40.0,4.0"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 4
    assert summary["rows"]["blank"] == 0
    # Structural malformed are not counted in 'malformed' (content)
    # They are implicitly excluded from 'sanitised'
    assert summary["rows"]["malformed"] == 0
    assert summary["rows"]["duplicated"] == 0
    assert summary["rows"]["sanitised"] == 2 # Only 1,A1 and 4,D4 are structurally correct
    assert summary["rows"]["valid"] == 2
    assert summary["rows"]["usable"] == 2
    assert summary["outcome"]["accepted"] == 2
    assert summary["outcome"]["rejected"] == 0 # Structural errors not explicitly in rejected
    assert len(df) == 2 # Only structurally correct rows form the DF

def test_detailed_analysis_with_content_malformed_rows():
    # Rows structurally correct but with bad data in critical fields
    raw_text = """order_id,sku,item_price,item_tax
1,A1,10.0,1.0
2,B2,invalid_sku,bad_price,2.0 # Malformed sku, bad_price
3,C3,30.0,3.0
4,,40.0,4.0 # Blank order_id
5,E5,50.0,invalid_tax # Malformed item_tax
"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 5
    assert summary["rows"]["blank"] == 0
    assert summary["rows"]["malformed"] == 3 # Rows 2, 4, 5 are content malformed
    assert summary["rows"]["duplicated"] == 0
    assert summary["rows"]["sanitised"] == 5
    assert summary["rows"]["valid"] == 2 # sanitised - malformed = 5 - 3 = 2
    assert summary["rows"]["usable"] == 2
    assert summary["outcome"]["accepted"] == 2
    assert summary["outcome"]["rejected"] == 3 # malformed (content) counted in rejected
    assert len(df) == 5

def test_detailed_analysis_with_duplicates():
    raw_text = """order_id,sku,item_price,item_tax,order_item_id
1,A1,10.0,1.0,item1
2,B2,20.0,2.0,item2
1,A1,10.0,1.0,item1 # Duplicate of item1
3,C3,30.0,3.0,item3
2,B2,20.0,2.0,item2 # Duplicate of item2
"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 5
    assert summary["rows"]["blank"] == 0
    assert summary["rows"]["malformed"] == 0
    assert summary["rows"]["duplicated"] == 2 # item1, item2
    assert summary["rows"]["sanitised"] == 5
    assert summary["rows"]["valid"] == 5
    assert summary["rows"]["usable"] == 3 # valid - duplicated = 5 - 2 = 3
    assert summary["outcome"]["accepted"] == 3
    assert summary["outcome"]["rejected"] == 2 # duplicated counted in rejected
    assert len(df) == 5 # DataFrame includes duplicates, but usable counts unique

def test_detailed_analysis_duplicates_without_order_item_id():
    raw_text = """order_id,sku,item_price,item_tax
1,A1,10.0,1.0
2,B2,20.0,2.0
1,A1,10.0,1.0 # Duplicate based on order_id+sku
3,C3,30.0,3.0
2,B2,20.0,2.0 # Duplicate based on order_id+sku
"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 5
    assert summary["rows"]["duplicated"] == 2
    assert summary["rows"]["usable"] == 3
    assert summary["outcome"]["accepted"] == 3
    assert summary["outcome"]["rejected"] == 2

def test_detailed_analysis_mixed_issues():
    raw_text = """order_id,sku,item_price,item_tax,order_item_id
1,A1,10.0,1.0,item1
,,,,, # Blank
2,B2,invalid_price,2.0,item2 # Content malformed
1,A1,10.0,1.0,item1 # Duplicate
3,C3,30.0 # Structural malformed
4,D4,40.0,4.0,item4
"""
    summary, df = _perform_detailed_analysis(raw_text)
    assert summary["rows"]["total"] == 6
    assert summary["rows"]["blank"] == 1
    assert summary["rows"]["malformed"] == 1 # Row 2
    assert summary["rows"]["duplicated"] == 1 # Row 4
    assert summary["rows"]["sanitised"] == 4 # Includes 1,A1,item1; 2,B2,item2; 1,A1,item1; 4,D4,item4
    assert summary["rows"]["valid"] == 3 # sanitised - malformed = 4 - 1
    assert summary["rows"]["usable"] == 2 # valid - duplicated = 3 - 1
    assert summary["outcome"]["accepted"] == 2
    assert summary["outcome"]["rejected"] == (1 + 1 + 1) # blank + malformed + duplicated = 3
    assert len(df) == 4 # Only structurally sound rows become DataFrame rows

# --- Tests for download_and_clean_csv function ---
def test_download_and_clean_csv_success(mocker):
    mock_url = "http://example.com/test.csv"
    mock_csv_content = """order_id,sku,item_price,item_tax
1,A1,10.0,1.0
2,B2,20.0,2.0"""
    mocker.patch('requests.get', return_value=MockResponse(mock_csv_content))

    file_id, df_cleaned, summary = download_and_clean_csv(mock_url)

    assert file_id in file_storage
    assert isinstance(df_cleaned, pd.DataFrame)
    assert not df_cleaned.empty
    assert summary["rows"]["total"] == 2
    assert summary["rows"]["usable"] == 2
    assert summary["durations"]["download_seconds"] >= 0
    assert summary["rows"]["encoding_errors"] == 0


def test_download_and_clean_csv_download_failure(mocker):
    mock_url = "http://example.com/nonexistent.csv"
    mocker.patch('requests.get', return_value=MockResponse("", status_code=404))

    with pytest.raises(ValueError, match="Error downloading file"):
        download_and_clean_csv(mock_url)

def test_download_and_clean_csv_encoding_failure(mocker):
    # Simulate a file that cannot be decoded by common encodings
    mock_url = "http://example.com/bad_encoding.csv"
    
    # Create content that will fail utf-8, latin1, ISO-8859-1, cp1252 strict decode
    # A simple way is to force an invalid byte sequence for common text encodings
    # For example, using bytes that are not valid UTF-8
    bad_bytes = b'\xc3\x28' # This is not valid UTF-8 (missing continuation byte)
    
    # We want to test that the `encoding_errors` count increases and eventually fails if no encoding works.
    # To do this, we'll patch `requests.Response.content.decode` directly to simulate errors.
    
    # First, let requests.get return some bytes
    mock_response_instance = MockResponse(content="some data", encoding='utf-8')
    mocker.patch('requests.get', return_value=mock_response_instance)

    # Now, patch the decode method of the response to always raise UnicodeDecodeError
    # for specific encodings if we need to control the exact count.
    # For simplicity, here we'll simulate a failure to decode with *any* of the tries
    # by making the initial decode to `raw_text` itself fail.
    
    # The current `download_and_clean_csv` tries multiple decodings.
    # If all fail, it raises a ValueError. The encoding_errors count will be the number of failed attempts.
    
    # Let's ensure our MockResponse returns content that will fail all attempts.
    # A simple way is to use a byte string directly and specify no 'encoding' argument
    # so `decode` isn't called initially by MockResponse but by `download_and_clean_csv`
    class ReallyBadMockResponse:
        def __init__(self, raw_bytes):
            self._content = raw_bytes
            self.status_code = 200
        @property
        def content(self):
            return self._content
        def raise_for_status(self):
            pass # No HTTP error

    mocker.patch('requests.get', return_value=ReallyBadMockResponse(raw_bytes=b'\xff\xfe\x00\x00')) # Bytes that will fail common text decoders

    with pytest.raises(ValueError, match="Could not decode file with any of the attempted encodings. Total encoding errors:"):
        download_and_clean_csv(mock_url)

def test_download_and_clean_csv_empty_file_after_download(mocker):
    mock_url = "http://example.com/empty.csv"
    mocker.patch('requests.get', return_value=MockResponse("")) # Empty content

    file_id, df_cleaned, summary = download_and_clean_csv(mock_url)
    assert summary["rows"]["total"] == 0
    assert summary["rows"]["usable"] == 0
    assert df_cleaned.empty
    assert file_id in file_storage