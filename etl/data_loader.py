import sqlite3

import pandas as pd


class DataLoader:
    def __init__(self, data, database_file):
        self.data = data
        self.database_file = database_file

    def load_processed_data(self, table_config):
        with self.connect_to_database() as conn:
            DataLoader.create_table(
                conn,
                table_config["main_table"],
                table_config["main_table_column_data_type"],
            )
            DataLoader.create_table(
                conn,
                table_config["history_table"],
                table_config["history_table_column_data_type"],
            )
            self.move_data_to_history_table(conn, table_config)
            DataLoader.query_main_table(conn, table_config)
            DataLoader.query_history_table(conn, table_config)

    def connect_to_database(self):
        return sqlite3.connect(self.database_file)

    @staticmethod
    def create_table(conn, table_name, column_data_type):
        cursor = conn.cursor()
        column_definitions = ", ".join(
            [f"{column} {data_type}" for column, data_type in column_data_type.items()]
        )
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_definitions}
            )
        """
        cursor.execute(create_table_sql)

    def move_data_to_history_table(self, conn, table_config):
        cursor = conn.cursor()

        for _, row in self.data.iterrows():
            primary_key_val = row[table_config["primary_key"]]

            cursor.execute(
                f"SELECT {table_config['primary_key']} FROM {table_config['main_table']} WHERE {table_config['primary_key']}=?",
                (primary_key_val,),
            )
            existing_primary_key = cursor.fetchone()

            if existing_primary_key:
                cursor.execute(
                    f"INSERT INTO {table_config['history_table']} SELECT * FROM {table_config['main_table']} WHERE {table_config['primary_key']}=?",
                    (primary_key_val,),
                )
                cursor.execute(
                    f"DELETE FROM {table_config['main_table']} WHERE {table_config['primary_key']}=?",
                    (primary_key_val,),
                )

            column_names = ", ".join(table_config["columns"])
            values = ", ".join(["?"] * len(table_config["columns"]))
            insert_sql = f"INSERT INTO {table_config['main_table']} ({column_names}) VALUES ({values})"
            cursor.execute(
                insert_sql, tuple(row[column] for column in table_config["columns"])
            )

    @staticmethod
    def query_main_table(conn, table_config):
        query_main_table_sql = f"SELECT count({table_config['primary_key']}) FROM {table_config['main_table']}"
        main_table_data = pd.read_sql_query(query_main_table_sql, conn)
        print(table_config["main_table"])
        print(main_table_data)

    @staticmethod
    def query_history_table(conn, table_config):
        query_history_table_sql = f"SELECT count({table_config['primary_key']}) FROM {table_config['history_table']}"
        history_table_data = pd.read_sql_query(query_history_table_sql, conn)
        print(table_config["history_table"])
        print(history_table_data)
