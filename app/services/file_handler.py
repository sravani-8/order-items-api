import pandas as pd
import requests
import io
import uuid
from datetime import datetime

# In-memory storage for processed files
# Each entry will be: file_storage[file_id] = {"data": df_cleaned, "summary": summary}
file_storage = {}

def download_and_clean_csv(url: str) -> tuple[str, pd.DataFrame, dict]:
    """
    Downloads a CSV from `url`, performs basic cleaning (drop duplicates, empty rows),
    and returns (file_id, cleaned_dataframe, summary_dict).
    """
    # 1) Download
    download_start = datetime.utcnow()
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        raise ValueError(f"Error downloading file: {e}")
    download_end = datetime.utcnow()
    download_seconds = (download_end - download_start).total_seconds()

    # 2) Read into DataFrame
    try:
        df = pd.read_csv(io.StringIO(response.text), encoding='utf-8', on_bad_lines='skip')
    except Exception as e:
        raise ValueError(f"Error reading CSV content: {e}")

    # 3) Basic row metrics before cleaning
    total_rows       = int(len(df))
    blank_rows       = int(df.isna().all(axis=1).sum())
    duplicated_rows  = int(df.duplicated().sum())

    # 4) Clean: drop duplicates and fully-empty rows
    processing_start = datetime.utcnow()
    df_cleaned = df.drop_duplicates().dropna(how='all')
    processing_end = datetime.utcnow()
    processing_seconds = (processing_end - processing_start).total_seconds()

    # 5) Build summary
    summary = {
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
        "durations": {
            "download_seconds": int(download_seconds),
            "processing_seconds": int(processing_seconds),
            "total_seconds": int(download_seconds + processing_seconds),
            "formatted": {
                "download": str(pd.to_timedelta(download_seconds, unit='s')),
                "processing": str(pd.to_timedelta(processing_seconds, unit='s'))
            }
        },
        "rows": {
            "total": total_rows,
            "blank": blank_rows,
            "duplicated": duplicated_rows
        }
    }

    # 6) Store cleaned data + summary in memory
    file_id = str(uuid.uuid4())
    file_storage[file_id] = {
        "data": df_cleaned,
        "summary": summary
    }

    return file_id, df_cleaned, summary
