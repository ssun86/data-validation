"""
This module sets up a FastAPI application with CORS middleware and includes a router for version 1 of the API.
It also defines a health check endpoint to check the server's uptime.

The module imports the necessary packages:
- `datetime` for working with dates and times
- `dotenv` for loading environment variables from a `.env` file
- `uvicorn` for running the ASGI server
- `FastAPI` and `CORSMiddleware` from the FastAPI package
- The `v1` router from the `app.routers` module

The FastAPI application is created and CORS middleware is added to allow requests from any origin.

The module defines a constant `PREFIX` as the base path for the API endpoints.
It includes the `v1.endpoints.router` with the specified `PREFIX`.

The module also defines a date format string and calculates the start time of the server in Hong Kong time.

The health check endpoint (`/api/health_check`) returns a dictionary containing a message with the server's uptime
and the start time in Hong Kong time.

If the module is run directly (`__main__`), it starts the uvicorn server on `0.0.0.0:8080`.
"""

import datetime
import os
from urllib.parse import urlencode

import dotenv
from pymongo import MongoClient
import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from loguru import logger


from app.routers import v1

dotenv.load_dotenv(".env.local")

mongodb_url = os.getenv("MONGODB_URL")


base_url = os.getenv("BASE_URL", "https://dev-search.ottuat.com")

if base_url.endswith("/"):
    base_url = base_url[:-1]

app = FastAPI()

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


PREFIX = "/api"

app.include_router(v1.endpoints.router, prefix=PREFIX)


DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
start_time = now_hk = datetime.datetime.now(
    datetime.timezone(datetime.timedelta(hours=8))
)
start_time = start_time.strftime(DATE_FORMAT)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Get client IP address (handling potential proxies)
    client_ip = request.client.host

    # Log the request details
    logger.info(
        f"Request: IP={client_ip}, Method={request.method}, URL={request.url}, Headers={request.headers}"
    )

    # Process the request and get the response
    response = await call_next(request)

    # Log the response details
    logger.info(f"Response: IP={client_ip}, Status={response.status_code}")

    return response


@app.get(f"{PREFIX}/health_check")
async def health_check():
    """
    Endpoint to check the server's uptime.

    Returns:
        dict: A dictionary containing a message indicating the server's uptime and the time it started.
              Returns 500 status code if the connection to MongoDB fails.
    """
    try:
        client = MongoClient(mongodb_url)
        db = client["search"]
        db.command("ping")
        response = "Connected to the MongoDB search database!"
        status_code = status.HTTP_200_OK  # Successful connection
    except Exception as e:
        response = f"Error connecting to MongoDB: {str(e)}"
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    finally:
        if "client" in locals():  # Only close if the connection was established
            client.close()

    return JSONResponse(
        content={"message": response, "start_hk_time": start_time},
        status_code=status_code,
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
