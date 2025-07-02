from typing import Any
import json
from fastapi import FastAPI, Form, Request, Query
from functools import partial
from fastapi.responses import Response,HTMLResponse
from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
# from fastapi.staticfiles import StaticFiles
# from fastapi.responses import RedirectResponse
# import uuid
from app.services.file_handler import download_and_clean_csv, file_storage
# No longer need this import as processing_stats.py is removed
# from app.services.processing_stats import compute_processing_stats 
from app.services.metrics_calculator import generate_metrics

# 1. Define a PrettyJSONResponse that always indents with 4 spaces
class PrettyJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        # Convert Pydantic models, datetimes, etc. to JSON-serializable types
        encoded = jsonable_encoder(content)
        # Dump with indentation
        pretty = json.dumps(encoded, indent=4, ensure_ascii=False)
        return pretty.encode("utf-8")

# 2. Create FastAPI app using our PrettyJSONResponse as the default
app = FastAPI(default_response_class=PrettyJSONResponse)

templates = Jinja2Templates(directory="app/templates")

# Serve the form
@app.get("/", response_class=HTMLResponse)
async def get_form(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})

# Handle form submission
@app.post("/upload")
async def upload_csv_url(csv_url: str = Form(...)):
    try:
        file_id, df_cleaned, summary = download_and_clean_csv(csv_url)
        # file_storage is already populated by download_and_clean_csv()
        return {"message": "File processed successfully", "file_id": file_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/order-items/uploads/{file_id}/processing-stats")
async def get_processing_stats(file_id: str):
    # Validate ID format
    if len(file_id) < 10:
        raise HTTPException(status_code=400, detail="Invalid file ID format.")

    entry = file_storage.get(file_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="File ID does not exist.")

    # The full processing stats are now directly available in the stored summary
    # No need to call compute_processing_stats anymore
    stats = entry["summary"]
    return {
        "uploaded_at": stats["uploaded_at"],
        "durations":   stats["durations"],
        "rows":        stats["rows"],
        "outcome":     stats["outcome"],
    }


@app.get("/api/v1/order-items/uploads/{file_id}/metrics")
async def get_metrics(file_id: str, groupby: str = Query(...)):
    # Validate ID format
    if len(file_id) < 10:
        raise HTTPException(400, "Invalid file ID format.")

    entry = file_storage.get(file_id)
    if entry is None:
        raise HTTPException(404, "File ID does not exist.")

    df = entry["data"]
    if df is None:
        raise HTTPException(409, "File is still being processed.")

    # Compute metrics (now returns start/end)
    try:
        grand_totals, metrics_list, start_date, end_date = generate_metrics(df.copy(), groupby)
    except ValueError as e:
        raise HTTPException(400, str(e))

    return {
        "group_by":     groupby,
        "start_date":   start_date,
        "end_date":     end_date,
        "uploaded_at": entry["summary"]["uploaded_at"],
        "grand_totals": grand_totals,
        "metrics":      metrics_list
    }