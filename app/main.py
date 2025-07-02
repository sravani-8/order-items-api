from fastapi import FastAPI, Form, Request, Query
from fastapi.responses import HTMLResponse
from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
# from fastapi.staticfiles import StaticFiles
# from fastapi.responses import RedirectResponse
# import uuid
from app.services.file_handler import download_and_clean_csv, file_storage
# No longer need this import as processing_stats.py is removed
# from app.services.processing_stats import compute_processing_stats 
from app.services.metrics_calculator import generate_metrics

app = FastAPI()

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
    return entry["summary"]


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