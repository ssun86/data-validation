import mysql.connector
from tqdm import tqdm


class MySQLConnector:
    def __init__(
        self,
        database,
        host,
        user,
        password,
    ) -> None:

        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = 3306
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
        )

    def get_cursor(self):
        if self.connection.is_connected():
            return self.connection.cursor()
        else:
            raise Exception("Connection is not established.")

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()

    def query(self, sql, batch_size=100):
        all_rows = []
        self.connect()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                columns = [col[0] for col in cursor.description]

                with tqdm(
                    total=cursor.rowcount, desc="MySQL Querying", leave=True, position=0
                ) as progress_bar:
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break

                        for row in rows:
                            dict_ = {}
                            for col, value in zip(columns, row):
                                dict_[col] = value
                            all_rows.append(dict_)

                        progress_bar.update(len(rows))
        except Exception as e:
            raise e
        finally:
            self.connection.close()
        return all_rows
