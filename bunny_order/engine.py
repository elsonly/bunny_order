import datetime as dt
from typing import Dict, DefaultDict, List, Deque, Tuple, Union
import os
import time
from collections import deque, defaultdict
from threading import Thread
import threading

from bunny_order.utils import logger, get_tpe_datetime
from bunny_order.database.data_manager import DataManager
from bunny_order.order_observer import OrderObserver
from bunny_order.models import (
    Strategy,
    SF31Order,
    Signal,
    Action,
    Trade,
    Order,
    SF31Position,
    QuoteSnapshot,
    Position,
    Contract,
    Event,
)
from bunny_order.config import Config
from bunny_order.order_manager import OrderManager
from bunny_order.exit_handler import ExitHandler
from bunny_order.risk_manager import RiskManager


class Engine:
    def __init__(
        self,
        debug: int,
        sync_interval: int,
        trade_start_time: dt.time,
        trade_end_time: dt.time,
    ):
        self.dm = DataManager()
        self.strategies: Dict[int, Strategy] = {}
        self.snapshots: Dict[str, QuoteSnapshot] = {}
        self.positions: Dict[int, Dict[str, Position]] = {}
        self.contracts: Dict[str, Contract] = {}
        self.unhandled_orders: Deque[SF31Order] = deque()
        # order_id -> Order
        self.order_callbacks: Dict[str, Order] = {}
        # order_id -> List[Trade]
        self.trade_callbacks: Dict[str, List[Trade]] = {}
        self.unhandled_trade_callbacks: Deque[Tuple[int, Trade]] = deque()

        # order manager
        self.q_order_manager_in: Deque[
            Tuple[Event, Union[Signal, Order, Trade]]
        ] = deque()
        self.om_active_event = threading.Event()
        self.om = OrderManager(
            strategies=self.strategies,
            contracts=self.contracts,
            unhandled_orders=self.unhandled_orders,
            q_in=self.q_order_manager_in,
            active_event=self.om_active_event,
        )
        self.__thread_om = Thread(target=self.om.run, name="order_manager")
        self.__thread_om.setDaemon(True)

        # order observer
        self.q_order_observer_out: Deque[
            Tuple[Event, Union[Signal, Order, Trade, List[SF31Position]]]
        ] = deque()

        self.observer = OrderObserver(
            strategies=self.strategies, q_out=self.q_order_observer_out
        )

        # exit handler
        self.q_exit_handler_in: Deque[Tuple[Event, Dict[str, QuoteSnapshot]]] = deque()
        self.q_exit_handler_out: Deque[Tuple[Event, Signal]] = deque()
        self.exit_handler_active_event = threading.Event()
        self.exit_handler = ExitHandler(
            strategies=self.strategies,
            positions=self.positions,
            contracts=self.contracts,
            q_in=self.q_exit_handler_in,
            q_out=self.q_exit_handler_out,
            active_event=self.exit_handler_active_event,
        )
        self.__thread_exit_handler = Thread(
            target=self.exit_handler.run, name="exit_handler"
        )
        self.__thread_exit_handler.setDaemon(True)

        # risk manager
        self.rm = RiskManager(
            strategies=self.strategies,
            contracts=self.contracts,
        )

        self.active = False
        self.next_update_contracts_dt = (
            dt.datetime.now().replace(hour=8, minute=25, second=0)
        )
        self.sync_interval = sync_interval
        self.trade_start_time = trade_start_time
        self.trade_end_time = trade_end_time
        self.debug = debug

    def on_signal(self, signal: Signal):
        logger.info(signal)
        self.rm.validate_signal(signal)
        if signal.rm_validated:
            self.q_order_manager_in.append((Event.Signal, signal))
        self.dm.save_signal(signal)

    def map_signal_id_and_order_id(self, order: Order):
        for _ in range(len(self.unhandled_orders)):
            sf31_order = self.unhandled_orders.pop()
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
            else:
                self.unhandled_orders.append(sf31_order)

        logger.warning(
            f"cannot map to sf31_order | order: {order}\n{self.unhandled_orders}"
        )

    def on_order_callback(self, order: Order):
        logger.info(order)
        self.map_signal_id_and_order_id(order)
        self.order_callbacks[order.order_id] = order
        self.dm.save_order(order)

    def on_trade_callback(
        self, trade: Trade, retry_counter: int = 0, max_retries: int = 5
    ):
        if trade.order_id in self.order_callbacks:
            logger.info(trade)
            trade.strategy = self.order_callbacks[trade.order_id].strategy
            self.dm.save_trade(trade)

        elif retry_counter >= 0 and retry_counter < max_retries:
            self.unhandled_trade_callbacks.append((retry_counter + 1, trade))

        else:
            self.dm.save_trade(trade)
            logger.warning(
                f"cannot map trade to order | trade: {trade}\n{self.unhandled_trade_callbacks}"
            )

    def on_positions_callback(self, positions: List[SF31Position]):
        self.dm.save_positions(positions)

    def sync(self):
        self.strategies.update(self.dm.get_strategies())
        self.positions.update(self.dm.get_positions())
        codes = []
        for strategy_id in self.positions:
            codes.extend(list(self.positions[strategy_id]))
        self.snapshots.update(self.dm.get_quote_snapshots(codes))
        if not self.contracts or (get_tpe_datetime() > self.next_update_contracts_dt):
            self.contracts.update(self.dm.get_contracts())
            self.next_update_contracts_dt += dt.timedelta(days=1)

    def reset(self):
        logger.info("reset")
        self.unhandled_orders.clear()
        self.order_callbacks.clear()
        self.trade_callbacks.clear()
        self.unhandled_trade_callbacks.clear()

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
        self.exit_handler.reset()

    def run(self):
        logger.info("Start Engine")
        self.sync()
        self.observer.start()
        self.__thread_om.start()
        self.__thread_exit_handler.start()
        prev_sync_ts = 0
        dt_8am = get_tpe_datetime().replace(hour=8, minute=0, second=0, microsecond=0)
        if get_tpe_datetime() >= dt_8am:
            next_reset_dt = dt_8am + dt.timedelta(days=1)
        else:
            next_reset_dt = dt_8am
        self.active = True
        while self.active:
            try:
                if get_tpe_datetime() >= next_reset_dt:
                    self.reset()
                    next_reset_dt += dt.timedelta(days=1)

                if not self.debug and get_tpe_datetime().weekday() >= 5:
                    time.sleep(10)
                    continue
                if not self.debug and (
                    get_tpe_datetime().time() < self.trade_start_time
                    or get_tpe_datetime().time() > self.trade_end_time
                ):
                    time.sleep(10)
                    continue

                ts = time.time()
                if ts - prev_sync_ts > self.sync_interval:
                    self.sync()
                    self.exit_handler.q_in.append((Event.Quote, self.snapshots))
                    prev_sync_ts = ts

                for _ in range(len(self.unhandled_trade_callbacks)):
                    retry_counter, trade = self.unhandled_trade_callbacks.pop()
                    self.on_trade_callback(trade, retry_counter=retry_counter)

                while self.q_order_observer_out:
                    event, data = self.q_order_observer_out.pop()
                    if event == Event.Signal:
                        self.on_signal(data)
                    elif event == Event.OrderCallback:
                        self.on_order_callback(data)
                    elif event == Event.TradeCallback:
                        self.on_trade_callback(data)
                    elif event == Event.PositionsCallback:
                        self.on_positions_callback(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

                while self.q_exit_handler_out:
                    event, data = self.q_exit_handler_out.pop()
                    if event == event.Signal:
                        self.on_signal(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

                time.sleep(0.01)
            except KeyboardInterrupt:
                self.active = False
                self.stop()
            except Exception as e:
                logger.exception(e)
                time.sleep(0.01)
        logger.info("Shutdown Engine")

    def stop(self):
        self.om_active_event.set()
        self.exit_handler_active_event.set()
        if self.__thread_om.is_alive():
            self.__thread_om.join(10)
        if self.__thread_exit_handler.is_alive():
            self.__thread_exit_handler.join(10)

    def __del__(self):
        self.stop()
