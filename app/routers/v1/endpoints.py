import os
import asyncio
from fastapi import APIRouter
from . import utils
from .search_engine import SearchEngine
from .mysql_connector import MySQLConnector
from enum import Enum
from typing import Any,Dict
from typing import List
from pydantic import BaseModel


ROUTE_NAME = "v1"

router = APIRouter(
    prefix=f"/{ROUTE_NAME}",
    tags=[ROUTE_NAME],
)
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_DB = os.environ["MYSQL_DB"]
MySQLConnector = MySQLConnector(
    database=MYSQL_DB, host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD
)

MONGODB_URL = os.environ["MONGODB_URL"]
search_engine = SearchEngine(mongodb_url=MONGODB_URL, mysql_connector=MySQLConnector)


class Item(BaseModel):
    series_ids: List[int]  # 接受请求体中的整数数组
    
class productItem(BaseModel):
    product_ids: List[int]  # 接受请求体中的整数数组

@router.post("/series")
async def series(series_ids: Item) -> Any:

    current_timestamp = utils.get_unix_timestamp()

    results = {
        "status": {
            "code": 0,
            "message": "It is working!",
        },
        "server": {"time": current_timestamp},
        "data": {},
    }

    async def search_task():
        results["data"]["series"] = await search_engine.search_series(
            series_ids=series_ids
        )

    await asyncio.gather(search_task())

    if results["data"]["series"] == None:
        results["data"]["code"] = 1
    else:
        results["data"]["code"] = 0

    return results

async def execute_search_task(task_name: str) -> Dict[str, Any]:
    current_timestamp = utils.get_unix_timestamp()
    results = {
        "status": {"code": 0, "message": f"It is working for {task_name}!"},
        "server": {"time": current_timestamp},
        "data": {},
    }

    async def search_task():
        if task_name == "seriesId":
            results["data"]["series_ids"] = await search_engine.search_data("series")
        elif task_name == "productId":
            results["data"]["product_ids"] = await search_engine.search_data("product")

    await asyncio.gather(search_task())

    if results["data"].get("series_ids") or results["data"].get("product_ids"):
        results["data"]["code"] = 1
    else:
        results["data"]["code"] = 0

    return results

@router.post("/seriesId")
async def seriesId() -> Any:
    return await execute_search_task("seriesId")

@router.post("/product")
async def product() -> Any:
    return await execute_search_task("productId")