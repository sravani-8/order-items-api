import pandas as pd
import requests
import io
import uuid
import csv
from datetime import datetime

# In-memory storage for processed files
# Each entry: file_storage[file_id] = {"data": df_cleaned, "summary": summary}
file_storage = {}

def download_and_clean_csv(url: str) -> tuple[str, pd.DataFrame, dict]:
    """
    Downloads CSV, counts encoding/malformed lines, cleans duplicates/blanks,
    and returns (file_id, cleaned DataFrame, summary dict).
    """
    # 1) Download raw bytes
    download_start = datetime.utcnow()
    try:
        resp = requests.get(url)
        resp.raise_for_status()
    except Exception as e:
        raise ValueError(f"Error downloading file: {e}")
    download_end = datetime.utcnow()
    download_secs = (download_end - download_start).total_seconds()

    # 2) Decode with replacement to catch encoding errors
    raw_text = resp.content.decode('utf-8', errors='replace')
    lines = raw_text.splitlines()

    # Count encoding errors as lines containing Unicode replacement char
    encoding_errors = sum('\ufffd' in line for line in lines)

    # 3) Parse CSV rows manually to catch malformed line counts
    reader = csv.reader(lines)
    all_rows = list(reader)
    if not all_rows:
        raise ValueError("CSV is empty or malformed header.")

    header = all_rows[0]
    data_rows = all_rows[1:]

    malformed = 0
    good_rows = []
    for row in data_rows:
        # if row length differs from header, consider malformed
        if len(row) != len(header):
            malformed += 1
        else:
            good_rows.append(row)

    total_rows = len(data_rows)

    # 4) Build DataFrame from good rows
    df = pd.DataFrame(good_rows, columns=header)

    # 5) Count blank rows (all empty or whitespace)
    df_replace = df.replace(r'^\s*$', pd.NA, regex=True)
    blank_rows = int(df_replace.isna().all(axis=1).sum())

    # 6) Count duplicates before cleaning
    duplicated = int(df.duplicated().sum())

    # 7) Clean: drop duplicates & fully-empty
    processing_start = datetime.utcnow()
    df_cleaned = df.drop_duplicates().dropna(how='all')
    processing_end = datetime.utcnow()
    processing_secs = (processing_end - processing_start).total_seconds()

    # 8) Build summary
    summary = {
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
        "durations": {
            "download_seconds": int(download_secs),
            "processing_seconds": int(processing_secs),
            "total_seconds": int(download_secs + processing_secs),
            "formatted": {
                "download": str(pd.to_timedelta(download_secs, unit='s')),
                "processing": str(pd.to_timedelta(processing_secs, unit='s'))
            }
        },
        "rows": {
            "total": total_rows,
            "blank": blank_rows,
            "malformed": malformed,
            "encoding_errors": encoding_errors,
            "duplicated": duplicated
        }
    }

    # 9) Store and return
    file_id = str(uuid.uuid4())
    file_storage[file_id] = {
        "data": df_cleaned,
        "summary": summary
    }
    return file_id, df_cleaned, summary
