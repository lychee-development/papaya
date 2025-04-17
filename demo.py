from fastapi import FastAPI, Request

app = FastAPI()

@app.get("/")
async def read_root():
    """
    Root endpoint.
    """
    return {"message": "Hello World"}

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    """
    GET endpoint to retrieve an item by ID (no validation).
    """
    return {"item_id": item_id, "description": f"This is item {item_id}"}

@app.post("/items/")
async def create_item(request: Request):
    """
    POST endpoint to create an item (no validation).
    Reads raw request body if needed, but doesn't process it.
    """
    # Example of accessing raw body if needed, but not required for this simple example
    # body = await request.body()
    # print(f"Received body: {body}")
    return {"message": "Item received"}

@app.post("/process/")
async def process_data(request: Request):
    """
    POST endpoint for some dummy processing (no validation).
    """
    # Example of accessing form data if needed, but not required
    # try:
    #     form_data = await request.form()
    #     print(f"Received form data: {form_data}")
    # except Exception:
    #     print("No form data or not form-encoded")

    # Example of accessing JSON data if needed, but not required
    # try:
    #     json_data = await request.json()
    #     print(f"Received JSON data: {json_data}")
    # except Exception:
    #     print("No JSON data or invalid JSON")

    return {"message": "Processing request received"}

@app.get("/status")
async def get_status():
    """
    GET endpoint to check status.
    """
    return {"status": "ok"}

# Example of how to run this app using uvicorn:
# uvicorn your_module_name:app --reload
