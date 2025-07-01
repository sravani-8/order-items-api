import pandas as pd
import requests
import uuid
import csv
from datetime import datetime
from io import StringIO

# In-memory storage for processed files
file_storage = {}

def download_and_clean_csv(url: str, chunksize: int = 100_000) -> tuple[str, pd.DataFrame, dict]:
    """
    Downloads CSV, counts encoding errors & malformed rows in one pass,
    then streams in pandas chunks to drop duplicates/blanks and count metrics.
    Returns (file_id, cleaned DataFrame, summary dict).
    """

    # 1) Download raw bytes and time it
    download_start = datetime.utcnow()
    try:
        resp = requests.get(url)
        resp.raise_for_status()
    except Exception as e:
        raise ValueError(f"Error downloading file: {e}")
    download_end = datetime.utcnow()
    download_secs = (download_end - download_start).total_seconds()

    # 2) Decode to string once for manual parsing
    raw_text = resp.content.decode("utf-8", errors="replace")
    lines = raw_text.splitlines()

    # Count encoding errors (replacement char)
    encoding_errors = sum("\ufffd" in line for line in lines)

    # Manual CSV parse to count malformed rows
    reader = csv.reader(lines)
    all_rows = list(reader)
    if not all_rows:
        raise ValueError("CSV is empty or malformed header.")
    header = all_rows[0]
    data_rows = all_rows[1:]

    malformed = sum(1 for row in data_rows if len(row) != len(header))
    total_rows = len(data_rows)

    # 3) Now stream-clean in pandas
    processing_start = datetime.utcnow()

    blank_rows = 0
    duplicated_rows = 0
    cleaned_chunks = []

    # We need to feed pandas only the well-formed lines:
    # build a small in-memory buffer of header + good rows, chunked
    good_lines = [" ,".join(header)]  # dummy join to match header columns
    for row in data_rows:
        if len(row) == len(header):
            # re-join row into CSV line
            good_lines.append(",".join(row))

    # Now use pandas.read_csv on the cleaned lines in chunks
    stream = StringIO("\n".join(good_lines))
    chunk_iter = pd.read_csv(
        stream,
        dtype=str,
        on_bad_lines="skip",
        chunksize=chunksize
    )

    for chunk in chunk_iter:
        # Count blank rows in this chunk
        blank_rows += int(
            chunk.replace(r"^\s*$", pd.NA, regex=True)
                 .isna()
                 .all(axis=1)
                 .sum()
        )
        # Count duplicates in this chunk
        duplicated_rows += int(chunk.duplicated().sum())

        # Drop duplicates & fully-empty
        cleaned_chunk = chunk.drop_duplicates().dropna(how="all")
        cleaned_chunks.append(cleaned_chunk)

    processing_end = datetime.utcnow()
    processing_secs = (processing_end - processing_start).total_seconds()

    # 4) Concatenate all cleaned chunks
    if cleaned_chunks:
        df_cleaned = pd.concat(cleaned_chunks, ignore_index=True)
    else:
        df_cleaned = pd.DataFrame(columns=header)

    # 5) Build summary
    summary = {
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
        "durations": {
            "download_seconds": int(download_secs),
            "processing_seconds": int(processing_secs),
            "total_seconds": int(download_secs + processing_secs),
            "formatted": {
                "download": str(pd.to_timedelta(download_secs, unit="s")),
                "processing": str(pd.to_timedelta(processing_secs, unit="s"))
            }
        },
        "rows": {
            "total": total_rows,
            "blank": blank_rows,
            "malformed": malformed, 
            "encoding_errors": encoding_errors,
            "duplicated": duplicated_rows
        }
    }

    # 6) Store and return
    file_id = str(uuid.uuid4())
    file_storage[file_id] = {
        "data": df_cleaned,
        "summary": summary
    }

    return file_id, df_cleaned, summary
