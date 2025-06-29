from datetime import datetime
import pandas as pd

def compute_processing_stats(summary: dict, df: pd.DataFrame) -> dict:
    # Use summary["uploaded_at"], summary["durations"]
    rows = {
        "total": int(len(df)),
        "blank": summary["rows"]["blank"],
        "malformed": summary.get("rows", {}).get("malformed", 0),
        "encoding_errors": summary.get("rows", {}).get("encoding_errors", 0),
        "duplicated": summary["rows"]["duplicated"],
        "sanitised": int(len(df)),
        "valid": int(len(df)),   # or your logic
        "usable": int(len(df))
    }
    outcome = {
        "accepted": rows["usable"],
        "rejected": rows["duplicated"]
    }

    return {
        "uploaded_at": summary["uploaded_at"],
        "durations": summary["durations"],
        "rows": rows,
        "outcome": outcome
    }
