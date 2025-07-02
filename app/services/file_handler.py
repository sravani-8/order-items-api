import pandas as pd
import requests
import uuid
import csv
from datetime import datetime
from io import StringIO
import re # Added for regex in cleaning

# In-memory storage for processed files
file_storage = {}

# --- New/Integrated Analysis Function (moved from previous iterations) ---
def _perform_detailed_analysis(raw_text: str, delimiter: str = ',') -> tuple[dict, pd.DataFrame]:
    """
    Performs detailed analysis on the raw text content of a CSV file.
    Calculates total, blank, malformed (content), encoding errors, duplicates,
    sanitised, valid, usable, accepted, and rejected rows.
    Returns the summary dictionary and the cleaned DataFrame.
    """
    
    # Initialize counters
    total_data_lines_in_file = 0
    blank_rows = 0
    lines_with_structural_error = 0 # Lines that csv.reader completely failed on due to wrong field count or csv.Error
    parsed_df_rows = [] # To store successfully parsed rows (structurally correct, not blank)
    header = []

    # Define columns for critical checks (for content malformed detection)
    critical_numeric_cols = ['item_price', 'item_tax']
    critical_text_cols = ['order_id', 'sku']
    
    # Split raw_text into lines, and identify header
    lines = raw_text.splitlines()
    if not lines:
        return {
            "rows": {
                "total": 0, "blank": 0, "malformed": 0, "encoding_errors": 0,
                "duplicated": 0, "sanitised": 0, "valid": 0, "usable": 0
            },
            "outcome": {"accepted": 0, "rejected": 0}
        }, pd.DataFrame() # Return empty DataFrame if no lines

    # Attempt to parse header from the first line
    temp_header_reader = csv.reader([lines[0]], delimiter=delimiter)
    try:
        header = next(temp_header_reader)
        header = [h.strip() for h in header]
    except (StopIteration, csv.Error):
        # If header itself is malformed or file is only header
        # Treat entire file as having structural errors or unreadable
        # Return a simplified summary and empty DataFrame
        return {
            "rows": {
                "total": len(lines) - 1 if len(lines) > 0 else 0,
                "blank": 0, "malformed": (len(lines) -1) if len(lines) > 0 else 0, # All data lines malformed structurally
                "encoding_errors": 0, "duplicated": 0, "sanitised": 0, "valid": 0, "usable": 0
            },
            "outcome": {"accepted": 0, "rejected": (len(lines) -1) if len(lines) > 0 else 0}
        }, pd.DataFrame(columns=[f'Unnamed: {i}' for i in range(len(header))]) if header else pd.DataFrame()


    expected_cols = len(header)
    data_lines = lines[1:] # All lines after the header
    total_data_lines_in_file = len(data_lines)

    for line in data_lines:
        row = []
        try:
            # Use csv.reader for each line to handle quoted fields correctly
            single_line_reader = csv.reader([line], delimiter=delimiter)
            row = next(single_line_reader)

            # Check for blank row first (all fields are empty/whitespace)
            if not any(field.strip() for field in row):
                blank_rows += 1
                continue 
            
            # Check for structural malformation (incorrect number of fields)
            if len(row) != expected_cols:
                lines_with_structural_error += 1
                continue 
            
            # If not blank and structurally correct, add to rows for DataFrame
            parsed_df_rows.append(row)
            
        except csv.Error:
            lines_with_structural_error += 1
            continue
        except Exception: # Catch any other unexpected error during single line parsing
            lines_with_structural_error += 1
            continue
            
    # Clean header for Pandas: avoid empty names, ensure uniqueness
    cleaned_header = []
    seen_names = {}
    for h in header:
        if h == '': 
            idx = seen_names.get('Unnamed', 0)
            new_name = f'Unnamed: {idx}'
            seen_names['Unnamed'] = idx + 1
            cleaned_header.append(new_name)
        elif h in seen_names:
            idx = seen_names[h]
            new_name = f'{h}.{idx}'
            seen_names[h] = idx + 1
            cleaned_header.append(new_name)
        else:
            seen_names[h] = 1
            cleaned_header.append(h)
            
    # Create DataFrame from structurally parsed and non-blank rows
    df = pd.DataFrame(parsed_df_rows, columns=cleaned_header)
    
    # --- Calculate metrics based on df ---
    
    # sanitised: Rows that were successfully parsed and are not blank.
    sanitised_rows = len(df) 
    
    # Duplicated Rows
    duplicated_rows = 0
    key_for_duplicates = 'order_item_id'
    
    if key_for_duplicates not in df.columns:
        # print(f"Warning: '{key_for_duplicates}' not found. Using ['order_id', 'sku'] for duplicate check.")
        key_for_duplicates = ['order_id', 'sku']
        for col in key_for_duplicates:
            if col not in df.columns:
                # print(f"Error: Required column '{col}' for duplicate check not found. Cannot accurately count duplicates.")
                duplicated_rows = 0 
                break
        else:
            duplicated_rows = df.duplicated(subset=key_for_duplicates, keep='first').sum()
    else:
        duplicated_rows = df.duplicated(subset=key_for_duplicates, keep='first').sum()

    # Malformed (Content-based): Rows in the DataFrame with critical data issues AFTER cleaning.
    df_for_content_check = df.copy()

    # Apply cleaning and type coercion for malformed content check
    for col in critical_numeric_cols:
        if col in df_for_content_check.columns:
            df_for_content_check[col] = df_for_content_check[col].astype(str).str.replace('"', '', regex=False)
            df_for_content_check[col] = df_for_content_check[col].str.replace(r'[\n\r]', '', regex=True)
            df_for_content_check[col] = df_for_content_check[col].str.replace(r'[^\d.]', '', regex=True)
            df_for_content_check[col] = pd.to_numeric(df_for_content_check[col], errors='coerce')
        # else: print(f"Warning: Critical numeric column '{col}' not found for malformed content check.")

    for col in critical_text_cols:
        if col in df_for_content_check.columns:
            df_for_content_check[col] = df_for_content_check[col].astype(str).str.replace(r'[^\x00-\x7F]+', '', regex=True).str.strip()
        # else: print(f"Warning: Critical text column '{col}' not found for malformed content check.")
    
    good_content_mask = pd.Series(True, index=df_for_content_check.index)

    if 'order_id' in df_for_content_check.columns:
        good_content_mask &= (df_for_content_check['order_id'].notna()) & (df_for_content_check['order_id'] != '')
    if 'sku' in df_for_content_check.columns:
        good_content_mask &= (df_for_content_check['sku'].notna()) & (df_for_content_check['sku'] != '')
    if 'item_price' in df_for_content_check.columns:
        good_content_mask &= (df_for_content_check['item_price'].notna())
    if 'item_tax' in df_for_content_check.columns:
        good_content_mask &= (df_for_content_check['item_tax'].notna())

    malformed_content_rows = (~good_content_mask).sum()

    valid_rows = sanitised_rows - malformed_content_rows
    usable_rows = max(0, valid_rows - duplicated_rows) 

    accepted_rows = usable_rows
    rejected_rows = blank_rows + malformed_content_rows + duplicated_rows

    # Ensure all counts are standard Python integers for JSON serialization
    output_data = {
        "rows": {
            "total": int(total_data_lines_in_file),
            "blank": int(blank_rows),
            "malformed": int(malformed_content_rows),
            "encoding_errors": 0, # This function assumes raw_text is already decoded
            "duplicated": int(duplicated_rows),
            "sanitised": int(sanitised_rows),
            "valid": int(valid_rows),
            "usable": int(usable_rows)
        },
        "outcome": {
            "accepted": int(accepted_rows),
            "rejected": int(rejected_rows)
        }
    }
    
    # Return the full DataFrame as well, as it's needed by metrics_calculator
    return output_data, df 


def download_and_clean_csv(url: str, chunksize: int = 100_000) -> tuple[str, pd.DataFrame, dict]:
    """
    Downloads CSV, and performs detailed analysis.
    Returns (file_id, cleaned DataFrame, summary dict).
    """

    download_start = datetime.utcnow()
    try:
        resp = requests.get(url)
        resp.raise_for_status()
    except Exception as e:
        raise ValueError(f"Error downloading file: {e}")
    download_end = datetime.utcnow()
    download_secs = (download_end - download_start).total_seconds()

    # Decode to string. Assuming utf-8 is preferred, handle errors.
    raw_text = ""
    encoding_errors_during_decode = 0
    encodings_to_try = ['utf-8', 'latin1', 'ISO-8859-1', 'cp1252']
    
    file_decoded_successfully = False
    for encoding in encodings_to_try:
        try:
            raw_text = resp.content.decode(encoding, errors='strict')
            file_decoded_successfully = True
            break
        except UnicodeDecodeError:
            encoding_errors_during_decode += 1
            continue
        except Exception: # Catch any other error during decoding
            encoding_errors_during_decode += 1
            continue
            
    if not file_decoded_successfully:
        raise ValueError(f"Could not decode file with any of the attempted encodings. Total encoding errors encountered: {encoding_errors_during_decode}")

    # Now, use our detailed analysis function which processes raw_text
    summary_data, df_cleaned = _perform_detailed_analysis(raw_text)

    # Update summary with encoding errors from the download phase
    summary_data["rows"]["encoding_errors"] = encoding_errors_during_decode
    
    # Add durations to the summary
    summary_data["uploaded_at"] = datetime.utcnow().isoformat() + "Z"
    
    # Note: `_perform_detailed_analysis` does not explicitly measure its own processing time.
    # For now, processing_seconds is set to 0. You might want to wrap _perform_detailed_analysis
    # with a timer if you need that metric.
    processing_secs = 0 
    summary_data["durations"] = {
        "download_seconds": int(download_secs),
        "processing_seconds": int(processing_secs), 
        "total_seconds": int(download_secs + processing_secs),
        "formatted": {
            "download": str(pd.to_timedelta(download_secs, unit="s")),
            "processing": str(pd.to_timedelta(processing_secs, unit="s"))
        }
    }

    # 6) Store and return
    file_id = str(uuid.uuid4())
    file_storage[file_id] = {
        "data": df_cleaned,
        "summary": summary_data # Use the fully calculated summary_data
    }

    return file_id, df_cleaned, summary_data