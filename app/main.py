from fastapi import FastAPI, Form, Request, Query
from fastapi.responses import HTMLResponse
from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
# from fastapi.staticfiles import StaticFiles
# from fastapi.responses import RedirectResponse
# import uuid
from app.services.file_handler import download_and_clean_csv, file_storage
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
        file_id = download_and_clean_csv(csv_url)
        return {"message": "file processes successfully","file_id": file_id}
    except ValueError as e:
        return{"Error": str(e)}
    # # Generate a UUID to simulate file ID
    # file_id = str(uuid.uuid4())

    # # For now, we will just return the file_id (we will add real logic later)
    # return {"message": "CSV URL received", "file_id": file_id, "url": csv_url}


@app.get("/api/v1/order-items/uploads/{file_id}/processing-stats")
async def get_processing_stats(file_id: str):
    if len(file_id) < 10:
        raise HTTPException(status_code=400, detail="Invalid file ID format.")

    file_entry = file_storage.get(file_id)
    if file_entry is None:
        raise HTTPException(status_code=404, detail="File ID does not exist.")

    return file_entry  # Return full detailed response


@app.get("/api/v1/order-items/uploads/{file_id}/metrics")
async def get_metrics(file_id: str, groupby: str = Query(...)):
    if len(file_id) < 10:
        return {"error": "Invalid file ID format."}

    file_entry = file_storage.get(file_id)
    if file_entry is None:
        return {"error": "File ID does not exist."}

    df = file_entry.get("data")
    if df is None:
        return {"error": "File is still being processed."}

    if groupby not in ["month", "year"]:
        return {"error": "Invalid groupby parameter. Use 'month' or 'year'."}

    grand_totals, metrics_list = generate_metrics(df.copy(), groupby)

    return {
        "group_by": groupby,
        "start_date": str(df["order_date"].min().date()),
        "end_date": str(df["order_date"].max().date()),
        "uploaded_at": file_entry.get("uploaded_at"),
        "grand_totals": grand_totals,
        "metrics": metrics_list
    }
