# Order Items Data Processor

This application provides a web interface to upload a CSV file via URL, process its data, and retrieve various statistics and metrics.

## Table of Contents

1.  [Features](https://www.google.com/search?q=%23features)
2.  [Project Structure](https://www.google.com/search?q=%23project-structure)
3.  [Setup and Installation](https://www.google.com/search?q=%23setup-and-installation)
      * [Prerequisites](https://www.google.com/search?q=%23prerequisites)
      * [Virtual Environment Setup](https://www.google.com/search?q=%23virtual-environment-setup)
      * [Install Dependencies](https://www.google.com/search?q=%23install-dependencies)
4.  [Running the Application](https://www.google.com/search?q=%23running-the-application)
      * [Start the FastAPI Server](https://www.google.com/search?q=%23start-the-fastapi-server)
5.  [Usage and Endpoints](https://www.google.com/search?q=%23usage-and-endpoints)
      * [1. Uploading a CSV File (Web Form)](https://www.google.com/search?q=%231-uploading-a-csv-file-web-form)
      * [2. Uploading a CSV File (API)](https://www.google.com/search?q=%232-uploading-a-csv-file-api)
      * [3. Get Processing Statistics](https://www.google.com/search?q=%233-get-processing-statistics)
      * [4. Get Sales Metrics](https://www.google.com/search?q=%234-get-sales-metrics)
6.  [Data Definitions (Processing Statistics)](https://www.google.com/search?q=%23data-definitions-processing-statistics)
7.  [Error Handling](https://www.google.com/search?q=%23error-handling)

## Features

  * **CSV Upload via URL**: Easily provide a URL to your CSV file for processing.
  * **Data Cleaning**: Handles blank rows, structural malformations, content-based malformations, and duplicate entries.
  * **Comprehensive Processing Statistics**: Get detailed counts of rows at various stages of processing.
  * **Sales Metrics Calculation**: Compute key sales metrics grouped by month or year.
  * **In-Memory Storage**: Processed file data is stored temporarily in memory for quick access.
  * **FastAPI Backend**: A robust and high-performance API.
  * **Jinja2 Templates**: Simple web interface for file upload.

## Project Structure

```
.
├── main.py
├── requirements.txt
└── app/
    ├── services/
    │   ├── file_handler.py
    │   └── metrics_calculator.py
    └── templates/
        └── form.html
```

  * `main.py`: The main FastAPI application file, handling routing and endpoint definitions.
  * `requirements.txt`: Lists all Python dependencies required for the project.
  * `app/services/`: Contains core business logic.
      * `file_handler.py`: Responsible for downloading CSVs, performing detailed data analysis (counting various row types), and storing processed data in memory.
      * `metrics_calculator.py`: Calculates sales-related metrics from the cleaned DataFrame.
  * `app/templates/`: Stores Jinja2 HTML temp
  lates.
      * `form.html`: The web form for CSV URL submission.

## Setup and Installation

Follow these steps to set up and run the application locally.

### Prerequisites

  * Python 3.8+
  * `pip` (Python package installer)

### Virtual Environment Setup

It's highly recommended to use a virtual environment to manage project dependencies.

1.  **Navigate to your project's root directory** (where `main.py` is located):

    ```bash
    cd your_project_directory
    ```

2.  **Create a virtual environment** (e.g., named `.venv`):

    ```bash
    python -m venv .venv
    ```

3.  **Activate the virtual environment**:

      * **On Windows:**
        ```bash
        .\.venv\Scripts\activate
        ```
      * **On macOS/Linux:**
        ```bash
        source ./.venv/bin/activate
        ```

    You should see `(.venv)` or similar prefix in your terminal prompt, indicating the virtual environment is active.

### Install Dependencies

With your virtual environment activated, install all required Python packages:

```bash
pip install -r requirements.txt
```

**`requirements.txt` content:**
(Please ensure your `requirements.txt` contains these. If you need to generate one, run `pip freeze > requirements.txt` after manually installing `fastapi`, `uvicorn`, `pandas`, `requests`, `jinja2`):

```
fastapi==0.111.0 # Or your preferred version
uvicorn==0.30.1 # Or your preferred version
pandas==2.2.2 # Or your preferred version
requests==2.32.3 # Or your preferred version
Jinja2==3.1.4 # Or your preferred version
python-multipart==0.0.9 # Required for form parsing in FastAPI
```

*(Adjust versions to match your exact setup if you have specific requirements.)*

## Running the Application

### Start the FastAPI Server

Once dependencies are installed, you can start the Uvicorn server:

```bash
uvicorn app.main:app --reload
```

  * `main`: Refers to the `main.py` file.
  * `app`: Refers to the `FastAPI()` instance named `app` inside `main.py`.
  * `--reload`: (Optional) Automatically reloads the server on code changes, useful for development.

You should see output similar to this, indicating the server is running:

```
INFO:     Will watch for changes in these directories: ['/path/to/your_project_directory']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [PID]
INFO:     Started server process [PID]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

The application will be accessible at `http://127.0.0.1:8000`.

## Usage and Endpoints

### 1\. Uploading a CSV File (Web Form)

Open your web browser and navigate to the application's root URL:

  * **URL**: `http://127.0.0.1:8000/`

You will see a simple form where you can enter the URL of your CSV file. Enter the URL and click "Submit".

Example URL (replace with your actual CSV file URL):
`https://example.com/path/to/your_data.csv`

The response will provide a `file_id` which you will use for subsequent API calls.

```json
{
  "message": "File processed successfully",
  "file_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef"
}
```

### 2\. Uploading a CSV File (API)

You can also use a tool like `curl` or Postman to upload a CSV URL directly to the API endpoint:

  * **Endpoint**: `POST /upload`
  * **Method**: `POST`
  * **Content-Type**: `application/x-www-form-urlencoded`
  * **Form Field**: `csv_url` (string)

**Example `curl` command:**

```bash
curl -X POST "http://127.0.0.1:8000/upload" \
-H "Content-Type: application/x-www-form-urlencoded" \
--data-urlencode "csv_url=https://your-csv-file-url.com/order_items_data.csv"
```

The response will be similar to the web form's response, providing a `file_id`.

### 3\. Get Processing Statistics

Once you have a `file_id` from the upload step, you can retrieve detailed processing statistics for that file.

  * **Endpoint**: `GET /api/v1/order-items/uploads/{file_id}/processing-stats`
  * **Method**: `GET`
  * **Path Parameter**: `file_id` (string, the ID returned from `/upload`)

**Example `curl` command:**

```bash
curl -X GET "http://127.0.0.1:8000/api/v1/order-items/uploads/ca7457af-24ff-4aee-90a5-e913da494ac8/processing-stats"
```

**Example JSON Response:**

```json
{
    "uploaded_at": "2025-07-02T11:13:20.172913Z",
    "durations": {
        "download_seconds": 129,
        "processing_seconds": 0,
        "total_seconds": 129,
        "formatted": {
            "download": "0 days 00:02:09.158386",
            "processing": "0 days 00:00:00"
        }
    },
    "rows": {
        "total": 8369150,
        "blank": 624065,
        "malformed": 115512,
        "encoding_errors": 0,
        "duplicated": 867428,
        "sanitised": 3318926,
        "valid": 3203414,
        "usable": 2335986
    },
    "outcome": {
        "accepted": 2335986,
        "rejected": 6033164
    }
}
```

### 4\. Get Sales Metrics

You can also retrieve sales metrics grouped by `month` or `year`.

  * **Endpoint**: `GET /api/v1/order-items/uploads/{file_id}/metrics`
  * **Method**: `GET`
  * **Path Parameter**: `file_id` (string, the ID returned from `/upload`)
  * **Query Parameter**: `groupby` (string, either `month` or `year`)

**Example `curl` command (grouped by month):**

```bash
curl -X GET "http://127.0.0.1:8000/api/v1/order-items/uploads/ca7457af-24ff-4aee-90a5-e913da494ac8/metrics?groupby=month"
```

**Example JSON Response:**

```json
{
    "group_by": "month",
    "start_date": "2025-01-01",
    "end_date": "2025-05-31",
    "uploaded_at": "2025-07-02T11:13:20.172913Z",
    "grand_totals": {
        "total_orders": 3074347,
        "gross_sales": 341859035.58000004,
        "net_sales": 384614250.4,
        "grand_total": 334420943.48,
        "most_popular_product_sku": "uxm-68468202",
        "least_popular_product_sku": "JnV-31918656Ú¿¡"
    },
    "metrics": [
        {
            "period": "2025-01",
            "total_orders": 630656,
            "gross_sales": 70146826.55,
            "net_sales": 78919850.69,
            "grand_total": 68618129.45,
            "most_popular_product_sku": "nli-44565082",
            "least_popular_product_sku": "ALv-61199172¬þ"
        },
        {
            "period": "2025-02",
            "total_orders": 568043,
            "gross_sales": 63195782.17,
            "net_sales": 71103521.39,
            "grand_total": 61817565.9,
            "most_popular_product_sku": "jUp-18407667",
            "least_popular_product_sku": "amB-95367729áúí"
        },
        {
            "period": "2025-03",
            "total_orders": 633044,
            "gross_sales": 70436231.69,
            "net_sales": 79242206.72,
            "grand_total": 68917049.36,
            "most_popular_product_sku": "amB-95367729",
            "least_popular_product_sku": "uxm-68468202ÁÖ"
        },
        {
            "period": "2025-04",
            "total_orders": 611383,
            "gross_sales": 67925220.83,
            "net_sales": 76424456.19,
            "grand_total": 66457754.79,
            "most_popular_product_sku": "uHc-41532924",
            "least_popular_product_sku": "uxm-68468202åô"
        },
        {
            "period": "2025-05",
            "total_orders": 631221,
            "gross_sales": 70154974.34,
            "net_sales": 78924215.41,
            "grand_total": 68610443.98,
            "most_popular_product_sku": "fDQ-79436990",
            "least_popular_product_sku": "raO-21663955éÍÜ"
        }
    ]
}
```

*(Note: Example metrics might vary based on the actual content of your CSV.)*

## Data Definitions (Processing Statistics)

The `rows` and `outcome` sections in the processing statistics provide a detailed breakdown of the data quality and processing results. Here are the precise definitions:

  * **`total`**: The total count of data lines identified in the original CSV file, excluding the header row.
  * **`blank`**: The number of data lines that were entirely empty or consisted solely of whitespace characters.
  * **`malformed`**: The number of rows that were successfully parsed into columns but contained invalid or unparseable data in critical fields (`order_id`, `sku`, `item_price`, `item_tax`) after basic cleaning (e.g., non-numeric values in numeric fields, or empty required text fields). This **does not** include rows that failed structural CSV parsing.
  * **`encoding_errors`**: The count of different encoding attempts that failed to successfully decode the raw file content before one succeeded.
  * **`duplicated`**: The number of duplicate rows identified based on the `order_item_id` field (or `order_id` + `sku` if `order_item_id` is unavailable). Only the first occurrence of a duplicate set is kept.
  * **`sanitised`**: The number of data lines that were successfully parsed by the CSV reader into a structured DataFrame. This includes rows that might still be content-malformed or duplicated, but excludes blank lines and lines that failed structural parsing (e.g., incorrect number of fields).
  * **`valid`**: The number of `sanitised` rows that passed all content validation rules (i.e., `sanitised` minus `malformed` content rows).
  * **`usable`**: The final number of rows that are considered clean and valid, after removing `duplicated` rows from the `valid` rows. These are the rows ready for further analytical processing.
  * **`accepted`**: The same count as `usable` rows. These are the rows that the system successfully processed and accepted.
  * **`rejected`**: The total count of rows that were ultimately excluded from the `total` set due to being `blank`, `malformed` (content-based), or `duplicated`.

## Error Handling

The application provides HTTP exceptions for various scenarios:

  * **400 Bad Request**: Invalid URL, invalid file ID format, missing required query parameters, or issues with data content (e.g., invalid `groupby` value).
  * **404 Not Found**: File ID does not exist in the in-memory storage.
  * **409 Conflict**: (Currently not explicitly used, but could indicate data still processing or unavailable).

