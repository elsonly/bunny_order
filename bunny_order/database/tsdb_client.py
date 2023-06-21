import time
from typing import List
import psycopg2
import psycopg2.extras as extras
import pandas as pd


class TSDBClient:
    def __init__(self, host: str, port: int, user: str, password: str, db: str):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__db = db
        self.reconnect_wait_seconds = 5
        self.reconnect_max_retires = 20000
        self._reconnect_retry = 0
        self.conn: psycopg2.connection = None
        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.__host,
            port=self.__port,
            user=self.__user,
            password=self.__password,
            database=self.__db,
        )
        if self.conn.closed == 0:
            self._reconnect_retry = 0

    def reconnect(self):
        while (
            not self.is_connected()
            and self._reconnect_retry < self.reconnect_max_retires
        ):
            self._reconnect_retry += 1
            print(f"reconnecting... | retry: {self._reconnect_retry}")
            self.connect()
            if not self.is_connected():
                time.sleep(self.reconnect_wait_seconds)

        if self.is_connected():
            print("connected")
        else:
            raise Exception("max reconnect_max_retires exceeded")

    def is_connected(self) -> bool:
        if not self.conn:
            return False
        if self.conn.closed != 0:
            return False
        return True

    def execute_query(self, query: str, out_type: str = None):
        """Execute a single query"""
        if not self.is_connected():
            self.reconnect()

        ret = 0  # Return value
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1

        # If this was a select query, return the result
        if "select" in query.lower() and "into" not in query.lower():
            ret = cursor.fetchall()
            if out_type == "df":
                cols = [x.name for x in cursor.description]
                ret = pd.DataFrame(ret, columns=cols)
            elif out_type == "dict":
                cols = [x.name for x in cursor.description]
                ret = [{col: row[k] for k, col in enumerate(cols)} for row in ret]

        cursor.close()
        return ret

    def execute_values_df(self, df: pd.DataFrame, table: str, page_size: int = 10000):
        """
        Using psycopg2.extras.execute_values() to insert the dataframe
        """
        if not self.is_connected():
            self.reconnect()
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ",".join(list(df.columns))
        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = self.conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples, page_size=page_size)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def execute_values(
        self, columns: List[str], data: List[tuple], table: str, page_size: int = 10000
    ):
        """
        Using psycopg2.extras.execute_values() to insert the List of tuple
        """
        if not self.is_connected():
            self.reconnect()
        # Comma-separated columns
        cols = ",".join(columns)
        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = self.conn.cursor()
        try:
            extras.execute_values(cursor, query, data, page_size=page_size)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def execute_batch_upsert_df(
        self,
        df: pd.DataFrame,
        table: str,
        conflict_cols: List[str],
        page_size: int = 1000,
    ):
        """
        Using psycopg2.extras.execute_batch() to upsert the dataframe
        """
        if not self.is_connected():
            self.reconnect()
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ",".join(list(df.columns))
        arg_placeholder = "%s," * len(df.columns)
        arg_placeholder = arg_placeholder[:-1]
        # update columns
        upd_sql = ", ".join(
            [f"{x} = EXCLUDED.{x}" for x in df.columns if x not in conflict_cols]
        )
        # conflict_cols
        conflict_sql = ",".join(conflict_cols)

        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s;" % (
            table,
            cols,
            arg_placeholder,
            conflict_sql,
            upd_sql,
        )
        cursor = self.conn.cursor()
        try:
            extras.execute_batch(cursor, query, tuples, page_size=page_size)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1
        cursor.close()
