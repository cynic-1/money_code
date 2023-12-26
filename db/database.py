import psycopg2


class Database:
    def __init__(self, connection_params):
        self.connection_params = connection_params

    def _connect(self):
        return psycopg2.connect(**self.connection_params)

    def store_data(self, data, table_name):
        with self._connect() as conn:
            with conn.cursor() as cur:
                # 创建表的 SQL 语句，这里需要根据 DataFrame 的结构来定义
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    open_time TIMESTAMP,
                    ema12 NUMERIC,
                    ema144 NUMERIC,
                    ema169 NUMERIC,     
                    ema576 NUMERIC,
                    ema676 NUMERIC,               
                );
                """
                cur.execute(create_table_query)
                conn.commit()

            # 使用 psycopg2 的扩展模块中的工具来将 DataFrame 导入 PostgreSQL
            from psycopg2.extensions import AsIs
            columns = AsIs(','.join(data.columns))
            values = [tuple(x) for x in data.values]

            insert_query = f"INSERT INTO {table_name} (%s) VALUES %s"
            psycopg2.extras.execute_values(cur, insert_query, (columns, values))
            conn.commit()
