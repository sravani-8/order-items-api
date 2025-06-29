from datetime import datetime
import pandas as pd

def compute_processing_stats(summary: dict, df: pd.DataFrame) -> dict:
    """
    Given the precomputed summary and the cleaned DataFrame, 
    returns the full processing-stats JSON structure.
    """
    rows_info = summary["rows"]
    total = int(rows_info["total"])
    blank = int(rows_info["blank"])
    malformed = int(rows_info["malformed"])
    encoding_errors = int(rows_info["encoding_errors"])
    duplicated = int(rows_info["duplicated"])
    sanitised = int(len(df))
    valid = sanitised  # after removing bad rows
    usable = sanitised

    return {
        "uploaded_at": summary["uploaded_at"],
        "durations": summary["durations"],
        "rows": {
            "total": total,
            "blank": blank,
            "malformed": malformed,
            "encoding_errors": encoding_errors,
            "duplicated": duplicated,
            "sanitised": sanitised,
            "valid": valid,
            "usable": usable
        },
        "outcome": {
            "accepted": usable,
            "rejected": duplicated
        }
    }
