import datetime as dt
from typing import Dict, List
import pandas as pd
from loguru import logger
from decimal import Decimal

from bunny_order.models import (
    Strategy,
    XQSignal,
    SF31Order,
    Order,
    Trade,
    Position,
)
from bunny_order.database.tsdb_client import TSDBClient
from bunny_order.config import Config


class DataManager:
    def __init__(self, verbose: bool = True):
        self.cli = TSDBClient(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            db=Config.DB_DATABASE,
        )
        self.verbose = verbose

    def convert_condition_to_sql_string(
        self, conditions: dict, sep: str = "and"
    ) -> str:
        cond_list = []
        for key, val in conditions.items():
            sql = ""
            if isinstance(val, bool):
                if val:
                    sql = f"{key}=true"
                else:
                    sql = f"{key}=false"
            elif isinstance(val, str):
                sql = f"{key}='{val}'"
            elif isinstance(val, (int, float, Decimal)):
                sql = f"{key}={val}"
            elif isinstance(val, (dt.date, dt.datetime, pd.Timestamp)):
                sql = f"{key}='{val.strftime('%Y-%m-%d')}'"
            elif isinstance(val, dt.time):
                sql = f"{key}='{val.strftime('%H:%M:%S.%f')}'"
            elif isinstance(val, list):
                if val:
                    if isinstance(val[0], str):
                        sql = f"""{key} in ( {",".join([f"'{x}'" for x in val])} ) """
                    elif isinstance(val[0], (int, float)):
                        sql = f"""{key} in ( {",".join([f"{x}" for x in val])} ) """
                    elif isinstance(val[0], (dt.date, dt.datetime, pd.Timestamp)):
                        sql = f"""{key} in ( {",".join([f"'{x.strftime('%Y-%m-%d')}'" for x in val])} ) """
                    else:
                        raise Exception(f"not handle type in list: {type(val)}")
            else:
                raise Exception(f"not handle type: {type(val)}")

            if sql:
                cond_list.append(sql)

        if cond_list:
            return f" {sep} ".join(cond_list)
        else:
            return " true "

    def get_max_datetime(
        self, table: str, conditions: Dict[str, str], time_col: str
    ) -> str:
        if conditions:
            sql_cond = self.convert_condition_to_sql_string(conditions=conditions)
            max_time = self.cli.execute_query(
                f"select max({time_col}) from {table} where {sql_cond}",
            )[0][0]
        else:
            max_time = self.cli.execute_query(f"select max({time_col}) from {table}")[
                0
            ][0]

        if isinstance(max_time, (dt.datetime, dt.date)):
            if "dt" in time_col:
                time_format = "%FT%X"
            else:
                time_format = "%F"
            return max_time.strftime(time_format)
        else:
            return max_time

    def _check_exists(self, table: str, conditions: dict):
        cond_sql = self.convert_condition_to_sql_string(conditions)
        counts = self.cli.execute_query(
            f"select count(*) from {table} where {cond_sql}",
        )[0][0]
        if counts:
            return True
        else:
            return False

    def save(
        self,
        table: str,
        df: pd.DataFrame,
        method: str,
        time_col: str = None,
        conflict_cols: List[str] = None,
        conditions: Dict[str, str] = None,
    ):
        """
        method (str): {direct, upsert, timeseries, if_not_exists}
        """
        if df.empty:
            return
        if self.verbose:
            logger.info(f"save {table} | size: {len(df)}")

        result = 1
        if method == "direct":
            result = self.cli.execute_values_df(df, table)

        elif method == "if_not_exists":
            if not self._check_exists(table=table, conditions=conditions):
                result = self.cli.execute_values_df(df, table)
            else:
                result = 0
                logger.warning("data already exists | skip insert")

        elif method == "timeseries":
            max_dt = self.get_max_datetime(table, conditions, time_col)
            if max_dt:
                df_save = df.loc[df[time_col] > max_dt]
            else:
                df_save = df
            result = self.cli.execute_values_df(df_save, table)

        elif method == "upsert":
            if not conflict_cols:
                raise Exception("conflict_cols is required")
            result = self.cli.excute_batch_upsert_df(
                df, table, conflict_cols=conflict_cols
            )

        if isinstance(result, int) and result == 1:
            raise Exception(f"save {table} | failed")
        elif isinstance(result, str) and "Error" in result:
            raise Exception(f"save {table} | failed", result)
        else:
            if self.verbose:
                logger.info(f"save {table} | success")

    def save_one(self, table: str, data: dict, method: str, conditions: dict = {}):
        result = 1
        if method == "direct":
            result = self.cli.execute_values(
                table=table,
                columns=data.keys(),
                data=[tuple(data.values())],
            )

        elif method == "if_not_exists":
            if not conditions:
                raise Exception("conditions is required")

            if not self._check_exists(table=table, conditions=conditions):
                result = self.cli.execute_values(
                    table=table,
                    columns=data.keys(),
                    data=[tuple(data.values())],
                )
            else:
                result = 0
                logger.info("data already exists | skip insert")

        if isinstance(result, int) and result == 1:
            raise Exception(f"save {table} | failed")
        elif isinstance(result, str) and "Error" in result:
            raise Exception(f"save {table} | failed", result)

    def update(self, table: str, uppdate_data: dict, conditions: dict):
        update_sql = self.convert_condition_to_sql_string(uppdate_data)
        cond_sql = self.convert_condition_to_sql_string(conditions)

        result = self.cli.execute_query(
            f"update {table} set {update_sql} where {cond_sql}",
        )

        if isinstance(result, int) and result == 1:
            raise Exception(f"save {table} | failed")
        elif isinstance(result, str) and "Error" in result:
            raise Exception(f"save {table} | failed", result)

    def get_strategies(self) -> Dict[str, Strategy]:
        data = self.cli.execute_query("select * from dealer.strategy", "dict")
        return {x["name"]: Strategy(**x) for x in data}

    def save_xq_signal(self, signal: XQSignal):
        self.save_one(
            table="dealer.xq_signals",
            data=signal.dict(),
            method="if_not_exists",
            conditions={
                "sdate": signal.sdate,
                "stime": signal.stime,
                "strategy_id": signal.strategy_id,
                "code": signal.code,
                "price": signal.price,
                "quantity": signal.quantity,
            },
        )

    def update_sf31_order(self, order: SF31Order):
        self.update(
            table="dealer.sf31_orders",
            uppdate_data={"order_id": order.order_id},
            conditions={
                "signal_id": order.signal_id,
                "strategy_id": order.strategy_id,
                "sfdate": order.sfdate,
                "sftime": order.sftime,
                "code": order.code,
                "price": order.price,
                "quantity": order.quantity,
                "action": order.action,
            },
        )

    def save_sf31_order(self, order: SF31Order):
        self.save_one(
            table="dealer.sf31_orders",
            data=order.dict(),
            method="if_not_exists",
            conditions={
                "signal_id": order.signal_id,
                "sfdate": order.sfdate,
                "sftime": order.sftime,
                "strategy_id": order.strategy_id,
                "code": order.code,
                "price": order.price,
                "quantity": order.quantity,
            },
        )

    def save_order(self, order: Order):
        self.save_one(
            table="dealer.orders_tmp",
            data=order.dict(),
            method="direct"
            # method="if_not_exists",
            # conditions={
            #     "order_date": order.order_date,
            #     "order_id": order.order_id,
            # },
        )

    def save_trade(self, trade: Trade):
        self.save_one(
            table="dealer.trades_tmp",
            data=trade.dict(),
            method="direct"
            # method="if_not_exists",
            # conditions={
            #     "order_id": trade.order_id,
            #     "trade_date": trade.trade_date,
            #     "trade_time": trade.trade_time,
            #     "strategy": trade.strategy,
            #     "code": trade.code,
            #     "price": trade.price,
            #     "qty": trade.qty,
            # },
        )

    def save_positions(self, position: List[Position]):
        pass