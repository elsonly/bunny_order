import datetime as dt
from typing import Dict, DefaultDict, List, Deque
import os
import time
from collections import deque, defaultdict

from bunny_order.utils import logger, get_tpe_datetime
from bunny_order.database.data_manager import DataManager
from bunny_order.order_observer import OrderObserver
from bunny_order.models import (
    Strategy,
    SF31Order,
    XQSignal,
    Action,
    Trade,
    Order,
)
from bunny_order.config import Config


class OrderManager:
    def __init__(self, enable_trade: bool = False, sync_interval: int = 5):
        self.enable_trade = enable_trade
        self.dm = DataManager()
        self.strategies: Dict[str, Strategy] = {}
        self.q_signals: Deque[XQSignal] = deque()
        self.q_orders: Deque[Order] = deque()
        self.q_trades: Deque[Trade] = deque()
        self.strategy_map: defaultdict[str, List[SF31Order]] = DefaultDict(list)
        # order_id -> strategy
        self.order_map: Dict[str, int] = {}

        self.observer = OrderObserver(
            strategies=self.strategies,
            q_signals=self.q_signals,
            q_orders=self.q_orders,
            q_trades=self.q_trades,
        )
        self.s31_orders_dir = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_SF31_ORDERS_DIR}"
        )
        self.active = False
        self.sync_interval = sync_interval
        self.max_trade_mapping_retries = 10
        self.trade_mapping_retry_wait = 1

    def update_strategies(self) -> None:
        self.strategies.update(self.dm.get_strategies())

    def get_strategy_name(self, strategy_id: int) -> str:
        for strategy in self.strategies.values():
            if strategy.id == strategy_id:
                return strategy.name
        return ""

    def on_signal(self, signal: XQSignal):
        logger.info(signal)
        self.dm.save_xq_signal(signal)
        orders = []
        orders = self.signal_to_sf31_orders(signal)
        for order in orders:
            self.send_sf31_order(order)

    def link_order(self, order: Order):
        for sf31_order in self.strategy_map[order.code]:
            if (
                order.order_date == sf31_order.sfdate
                and order.code == sf31_order.code
                and order.action == sf31_order.action
                and order.order_qty == sf31_order.quantity
                and order.order_price == sf31_order.price
                and order.order_type == sf31_order.order_type
            ):
                sf31_order.order_id = order.order_id
                order.strategy = sf31_order.strategy_id
                self.dm.update_sf31_order(sf31_order)
                return

        logger.error(
            f"cannot link to sf31_order | order: {order}\nsf31_order: {self.strategy_map[order.code]}"
        )

    def on_order(self, order: Order):
        logger.info(order)
        self.link_order(order)
        self.order_map[order.order_id] = order.strategy
        self.dm.save_order(order)

    def on_trade(self, trade: Trade):
        logger.info(trade)
        for _ in range(self.max_trade_mapping_retries):
            if trade.order_id in self.order_map:
                trade.strategy = self.order_map[trade.order_id]
                self.dm.save_trade(trade)
                return
            time.sleep(self.trade_mapping_retry_wait)
        self.dm.save_trade(trade)
        logger.error(f"cannot map trade to order | trade: {trade}")

    def leverage_adjustment(self, signal: XQSignal) -> int:
        return signal.quantity

    def intraday_order_adjustment(self, signal: XQSignal) -> int:
        return signal.price - 1

    def signal_to_sf31_orders(self, signal: XQSignal) -> List[SF31Order]:
        orders = []
        order_time = get_tpe_datetime().time()
        adj_qty = self.leverage_adjustment(signal)

        # split signal into 2 orders
        order1 = SF31Order(
            signal_id=signal.id,
            sfdate=signal.sdate,
            sftime=order_time,
            strategy_id=signal.strategy_id,
            security_type=signal.security_type,
            code=signal.code,
            order_type=signal.order_type,
            action=signal.action,
            quantity=int(0.5 * adj_qty),
            price=signal.price,
        )
        orders.append(order1)

        order2 = SF31Order(
            signal_id=signal.id,
            sfdate=signal.sdate,
            sftime=order_time,
            strategy_id=signal.strategy_id,
            security_type=signal.security_type,
            code=signal.code,
            order_type=signal.order_type,
            action=signal.action,
            quantity=adj_qty - int(0.5 * adj_qty),
            price=self.intraday_order_adjustment(signal),
        )
        orders.append(order2)

        return orders

    def send_sf31_order(self, order: SF31Order):
        """
        N12,Stock,1684143670.093469,2882,ROD,B,1,43.10
        """
        logger.info(order)
        strategy = self.get_strategy_name(order.strategy_id)
        ts = dt.datetime.combine(order.sfdate, order.sftime).timestamp()
        if not os.path.exists(f"{self.s31_orders_dir}"):
            os.mkdir(f"{self.s31_orders_dir}")
        if not os.path.exists(f"{self.s31_orders_dir}/{strategy}"):
            os.mkdir(f"{self.s31_orders_dir}/{strategy}")

        if order.action == Action.Buy:
            path = f"{self.s31_orders_dir}/{strategy}/Buy.log"
        elif order.action == Action.Sell:
            path = f"{self.s31_orders_dir}/{strategy}/Sell.log"
        if self.enable_trade:
            with open(path, "a") as f:
                f.write(
                    (
                        f"{order.signal_id},{order.security_type},{ts},{order.code},"
                        f"{order.order_type},{order.action},{order.quantity},{order.price}\n"
                    )
                )
            self.strategy_map[order.code].append(order)
            self.dm.save_sf31_order(order)

    def sync(self):
        self.update_strategies()

    def reset(self):
        logger.info("reset")
        self.strategy_map.clear()

        # xq_signal_dir = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_XQ_SIGNALS_DIR}"
        # if os.path.exists(xq_signal_dir):
        #     for file in os.listdir(xq_signal_dir):
        #         os.remove(f"{xq_signal_dir}/{file}")

        # order_path = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}/{Config.OBSERVER_ORDER_CALLBACK_FILE}"
        # if os.path.exists(order_path):
        #     with open(order_path, "r+") as f:
        #         _ = f.truncate(0)

        # trade_path = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}/{Config.OBSERVER_TRADE_CALLBACK_FILE}"
        # if os.path.exists(trade_path):
        #     with open(trade_path, "r+") as f:
        #         _ = f.truncate(0)

        # sf31_order_path = (
        #     f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_SF31_ORDERS_DIR}"
        # )
        # for root, dir, files in os.walk(sf31_order_path):
        #     for file in files:
        #         if file.endswith(".log"):
        #             file_path = f"{root}/{file}"
        #             with open(file_path, "r+") as f:
        #                 _ = f.truncate(0)

        self.observer.reset_checkpoints()

    def run(self):
        logger.info("Start Order Manager")
        self.observer.start()
        prev_sync_ts = 0
        dt_8am = get_tpe_datetime().replace(hour=8, minute=0, second=0, microsecond=0)
        if get_tpe_datetime() >= dt_8am:
            next_reset_dt = dt_8am + dt.timedelta(days=1)
        else:
            next_reset_dt = dt_8am

        self.active = True
        while self.active:
            try:
                ts = time.time()
                while self.q_signals:
                    signal = self.q_signals.pop()
                    self.on_signal(signal)

                while self.q_orders:
                    order = self.q_orders.pop()
                    self.on_order(order)

                while self.q_trades:
                    trade = self.q_trades.pop()
                    self.on_trade(trade)

                if ts - prev_sync_ts > self.sync_interval:
                    self.sync()
                    prev_sync_ts = ts

                if get_tpe_datetime() >= next_reset_dt:
                    self.reset()
                    next_reset_dt += dt.timedelta(days=1)

                time.sleep(0.01)
            except KeyboardInterrupt:
                self.active = False
            except Exception as e:
                logger.exception(e)
        logger.info("Shutdown Order-Manager")
