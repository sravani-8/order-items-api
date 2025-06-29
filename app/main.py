from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
# from fastapi.staticfiles import StaticFiles
# from fastapi.responses import RedirectResponse
# import uuid
from app.services.file_handler import download_and_clean_csv, file_storage

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

