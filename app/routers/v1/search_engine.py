import os
import dotenv
from pymongo import MongoClient
from .mysql_connector import MySQLConnector
from loguru import logger
from .sqls import series_sql, product_sql, product_id_sql
from typing import List

dotenv.load_dotenv()

class SearchEngine:
    def __init__(self, mysql_connector: MySQLConnector, mongodb_url=None) -> None:
        # mongodb配置
        if mongodb_url is None:
            mongodb_url = os.getenv("MONGODB_URL")
        self.client = MongoClient(mongodb_url)
        self.db = self.client["search"]
        self.series_collection = self.db["series"]
        self.product_collection = self.db["product"]
        self.actor_collection = self.db["tag_actor"]

        # mysql配置
        self.mysql_connector = mysql_connector

    async def search_series(self, series_ids: List[int]):

        search_pipeline = {"_id": {"$in": series_ids.series_ids}}

        mongo_results = self.series_collection.find(search_pipeline)

        mysql_results = self.series_etl(series_ids)
        mongo_list = list(mongo_results)

        # 比较数组
        differences = self.compare_objects_by_id(mysql_results, mongo_list)

        # 打印结果
        if differences:
            logger.info("Objects are different in the following fields:")
            for id_, diffs in differences.items():
                logger.info(f"ID: {id_}")
                for key, values in diffs.items():
                    new_log = f"  Field '{key}': mysql has {values['mysql']}, mongo has {values['mongo']}"
                    logger.error(new_log)
            return differences
        else:
            logger.info("Objects are equal.")

        return None

    async def search_data(self, table_name: str) -> bool:
        self.mysql_connector.connect()
        logger.info("get ids start !")

        # 获取mysql id
        mysql_ids = self.fetch_mysql_data(table_name)
        mongo_ids = self.fetch_mongo_data(table_name)

        logger.info("get ids end !")

        # 比较ID集合是否一致
        logger.info("compare ids start !")
        ids_match = self.compare_ids(table_name, mysql_ids, mongo_ids)
        logger.info("compare ids end !")

        #compare field and value.
        logger.info("compare fields start !")
        self.compare_fields(table_name, mysql_ids)
        logger.info("compare fields end !")

        self.mysql_connector.close()

        return ids_match

    def fetch_mysql_data(
        self, table_name, schedule_end_time=1736219420, batch_size=5000
    ):
        # 连接到MySQL数据库

        cursor = self.mysql_connector.get_cursor()

        last_id = None
        mysql_ids = set()

        while True:
            # 构建查询语句
            if last_id is None:
                query = f"SELECT {table_name}_id FROM {table_name} WHERE is_deleted = 0 AND schedule_end_time > {schedule_end_time} ORDER BY {table_name}_id ASC LIMIT %s"
                params = (batch_size,)
            else:
                query = f"SELECT {table_name}_id FROM {table_name} WHERE {table_name}_id > %s and is_deleted = 0 AND schedule_end_time > {schedule_end_time} ORDER BY {table_name}_id ASC LIMIT %s"
                params = (last_id, batch_size)

            # 构建查询语句
            cursor.execute(query, params)

            # 获取结果
            results = cursor.fetchall()
            if not results:
                break

            for row in results:
                last_id = row[0]
                mysql_ids.add(str(row[0]))  # 将ID转换为字符串，以匹配MongoDB的ID格式

        cursor.close()
        return mysql_ids

    def fetch_mysql_by_id(self, table_name, id):
        cursor = self.mysql_connector.get_cursor()
        query = None
        params = (id,)

        if table_name == "product":
            query = "SELECT product_id,last_modified_time FROM product WHERE product_id = %s"
        elif table_name == "series":
            query = "SELECT series_id,last_modified_time FROM series WHERE series_id = %s"

        if query is None:
            return None
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            if not results:
                return None
            return results[0]
        except Exception as e:
            return None
        finally:
            cursor.close()


    def fetch_mongo_by_id(self, table_name, id):
        # 获取对应的collection
        collection = getattr(self, f"{table_name}_collection", None)
        if collection is None:
            raise ValueError(f"Invalid table name: {table_name}")

        # 构建查询条件
        query = {"_id": id}

        # 查询记录，并只保留 _id 和 last_modified_time 字段
        projection = {"_id": 1, "last_modified_time": 1}

        record = collection.find_one(query, projection=projection)

        return record


    def fetch_mongo_data(
        self, table_name, schedule_end_time=1736219420, batch_size=5000
    ):

        last_id = None
        mongo_ids = set()

        while True:
            # 构建查询条件
            query = {
                "is_deleted": 0,
                "schedule_end_time": {"$gt": schedule_end_time},
                "_id": {"$gt": last_id},
            }
            if last_id == None:
                if table_name == "product":
                    cursor = (
                        self.product_collection.find(
                            {
                                "is_deleted": 0,
                                "schedule_end_time": {"$gt": schedule_end_time},
                            },
                            projection={"_id": 1},
                        )
                        .sort("_id", 1)
                        .limit(batch_size)
                    )
                elif table_name == "series":
                    cursor = (
                        self.series_collection.find(
                            {
                                "is_deleted": 0,
                                "schedule_end_time": {"$gt": schedule_end_time},
                            },
                            projection={"_id": 1},
                        )
                        .sort("_id", 1)
                        .limit(batch_size)
                    )
            else:
                if table_name == "product":
                    cursor = (
                        self.product_collection.find(query, projection={"_id": 1})
                        .sort("_id", 1)
                        .limit(batch_size)
                    )
                elif table_name == "series":
                    cursor = (
                        self.series_collection.find(query, projection={"_id": 1})
                        .sort("_id", 1)
                        .limit(batch_size)
                    )
            results = list(cursor)

            if not results:
                break

            for doc in results:
                last_id = doc["_id"]
                mongo_ids.add(str(doc["_id"]))  # 将ID转换为字符串，以匹配MySQL的ID格式

        return mongo_ids

    def compare_fields(self, table_name, mysql_ids):
        logger.info("compare fields start !")

        for id in mysql_ids:
            row_in_mysql = self.fetch_mysql_by_id(table_name, id)
            if row_in_mysql is None:
                continue

            row_in_mongo = self.fetch_mongo_by_id(table_name, id)
            if row_in_mongo is None:
                continue

            if row_in_mongo.last_modified_time == row_in_mysql.last_modified_time:
                continue

            filename = f"{table_name}_last_modified_time_mismatch.txt"
            with open(filename, "a") as file:
                file.write(f"{id}\n")

        logger.info("compare fields end !")

    def compare_ids(self, table_name, mysql_ids, mongo_ids):
        if mysql_ids != mongo_ids:
            logger.warning("ID lists are not matching.")
            mismatched_mysql = mysql_ids - mongo_ids
            mismatched_mongo = mongo_ids - mysql_ids
            # 将不匹配的ID写入文本文件
            filename = f"{table_name}_id_mismatch.txt"
            with open(filename, "w") as file:
                file.write(f"mysql_ids count: {len(mysql_ids)}\n")
                file.write(f"mongo_ids count: {len(mongo_ids)}\n")
                file.write(f"MySQL IDs not in MongoDB count: {len(mismatched_mysql)}\n")
                file.write(f"MySQL IDs not in MongoDB:\n{mismatched_mysql}\n\n")
                file.write(f"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
                file.write(f"MongoDB IDs not in MySQL count: {len(mismatched_mongo)}\n")
                file.write(f"MongoDB IDs not in MySQL:\n{mismatched_mongo}\n")
        else:
            logger.info("ID lists are completely matching.")
        return mysql_ids == mongo_ids

    def series_etl(self, series_ids: List[int]):

        if not series_ids:
            return

        # 处理ids
        # 直接将类型注解转换为字符串
        list_type_str = str(series_ids.series_ids)

        # 去掉字符串中的中括号
        series_ids_str = list_type_str.replace("[", "").replace("]", "")

        logger.info(f"Series with below ids will be updated: {series_ids_str}")

        series_etl_sql = series_sql.format(
            series_ids_str, series_ids_str, series_ids_str, series_ids_str
        )

        series_data = self.mysql_connector.query(series_etl_sql)

        fields_to_split = ["keyword", "actor_names", "alternative_names", "tag_names"]

        for row in series_data:
            for field in fields_to_split:
                if row[field]:
                    row[field] = [word.strip() for word in set(row[field].split(","))]

        return series_data

    def product_etl(self, product_ids: List[int]):

        if not product_ids:
            return

        # 处理ids
        # 直接将类型注解转换为字符串
        list_type_str = str(product_ids.product_ids)

        # 去掉字符串中的中括号
        product_ids_str = list_type_str.replace("[", "").replace("]", "")

        logger.info(f"product with below ids will be updated: {product_ids_str}")

        product_etl_sql = product_sql.format(
            product_ids_str, product_ids_str, product_ids_str, product_ids_str
        )

        product_data = self.mysql_connector.query(product_etl_sql)

        fields_to_split = ["keyword", "guest_tag_names"]

        for row in product_data:
            for field in fields_to_split:
                if row[field] and row[field] is not None:
                    row[field] = [word.strip() for word in set(row[field].split(","))]

        return product_data

    def compare_objects_by_id(self, mysql_data, mongo_data):
        # 创建一个以id为键，对象为值的字典，方便查找
        mysql_map = {obj["_id"]: obj for obj in mysql_data}
        mongo_map = {obj["_id"]: obj for obj in mongo_data}

        # 找出所有匹配的id
        all_ids = set(mysql_map.keys()).union(set(mongo_map.keys()))

        # 比较匹配id的对象
        differences = {}
        for id_ in all_ids:
            obj1 = mysql_map.get(id_)
            obj2 = mongo_map.get(id_)
            if obj1 and obj2:
                # 比较两个对象的其他字段
                for key in set(obj1.keys()).union(set(obj2.keys())):
                    if key != "_id":  # 假设 '_id' 是 MongoDB 的默认 ID 字段
                        val1 = obj1.get(key)
                        val2 = obj2.get(key)
                        # 如果字段值是列表，比较元素集合是否相等，忽略顺序
                        if isinstance(val1, list) and isinstance(val2, list):
                            if sorted(val1) != sorted(val2):
                                if id_ not in differences:
                                    differences[id_] = {}
                                differences[id_][key] = {"mysql": val1, "mongo": val2}
                        elif val1 != val2:  # 非列表字段直接比较
                            if id_ not in differences:
                                differences[id_] = {}
                            differences[id_][key] = {"mysql": val1, "mongo": val2}
            elif obj1 or obj2:
                # 如果只有一个对象中有这个id，也记录下来
                if id_ not in differences:
                    differences[id_] = {}
                differences[id_]["existence"] = {
                    "mysql": bool(obj1),
                    "mongo": bool(obj2),
                }

        return differences
