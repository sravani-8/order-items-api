import pandas as pd
import requests
import io
import uuid

# In-memory storage for processed files (replace with DB or file system in real apps)
file_storage = {}

def download_and_clean_csv(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
    except Exception as e:
        raise ValueError(f"Error downloading file: {e}")

    try:
        # Attempt to read CSV using utf-8
        df = pd.read_csv(io.StringIO(response.text), encoding='utf-8', on_bad_lines='skip')
    except Exception as e:
        raise ValueError(f"Error reading CSV content: {e}")

    original_row_count = len(df)

    # Drop duplicates and rows with all empty values
    df = df.drop_duplicates().dropna(how='all')
    cleaned_row_count = len(df)

    # TODO: log discarded rows in separate file

    # Generate a unique file_id and store the cleaned dataframe
    file_id = str(uuid.uuid4())
    file_storage[file_id] = {
        "data": df,
        "original_rows": original_row_count,
        "cleaned_rows": cleaned_row_count
    }

    return file_id
