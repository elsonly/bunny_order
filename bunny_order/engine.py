import datetime as dt
from typing import Dict, List, Deque, Tuple, Union
import os
import time
from collections import deque
from threading import Thread
import threading

from bunny_order.utils import (
    logger,
    get_tpe_datetime,
    is_trade_time,
    is_trade_date,
    get_next_schedule_time,
    is_signal_time,
    is_sync_time,
)
from bunny_order.database.data_manager import DataManager
from bunny_order.order_observer import OrderObserver
from bunny_order.models import (
    Strategy,
    SF31Order,
    Signal,
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
from bunny_order.common import Strategies, Snapshots, Positions, Contracts


class Engine:
    def __init__(
        self,
        debug: int,
        sync_interval: int,
        snapshot_interval: int,
    ):
        self.dm = DataManager()
        self.strategies = Strategies()
        self.snapshots = Snapshots()
        self.positions = Positions()
        self.contracts = Contracts()
        self.unhandled_orders: Deque[SF31Order] = deque()
        # order_id -> Order
        self.order_callbacks: Dict[str, Order] = {}
        self.unhandled_order_callbacks: Deque[Tuple[int, Order]] = deque()
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
            strategies=self.strategies,
            q_out=self.q_order_observer_out,
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
            positions=self.positions,
        )

        self.active = False
        self.sync_interval = sync_interval
        self.snapshot_interval = snapshot_interval
        self.debug = debug
        self.init_checkpoints()

    def init_checkpoints(self):
        if not os.path.exists(Config.CHECKPOINTS_DIR):
            os.mkdir(Config.CHECKPOINTS_DIR)

    def on_signal(self, signal: Signal):
        logger.info(signal)
        self.rm.validate_signal(signal)
        if signal.rm_validated:
            self.q_order_manager_in.append((Event.Signal, signal))
        self.dm.save_signal(signal)

    def map_signal_id_and_order_id(self, order: Order) -> bool:
        for _ in range(len(self.unhandled_orders)):
            sf31_order = self.unhandled_orders.popleft()
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
                return True
            else:
                self.unhandled_orders.append(sf31_order)

        return False

    def on_order_callback(
        self, order: Order, retry_counter: int = 0, max_retries: int = 10
    ):
        if self.map_signal_id_and_order_id(order):
            logger.info(order)
            self.order_callbacks[order.order_id] = order
            self.dm.save_order(order)

        elif retry_counter >= 0 and retry_counter < max_retries:
            self.unhandled_order_callbacks.append((retry_counter + 1, order))

        else:
            self.order_callbacks[order.order_id] = order
            logger.warning(f"cannot map to sf31_order | order: {order}")
            self.dm.save_order(order)

    def on_trade_callback(
        self, trade: Trade, retry_counter: int = 0, max_retries: int = 20
    ):
        if trade.order_id in self.order_callbacks:
            trade.strategy = self.order_callbacks[trade.order_id].strategy
            logger.info(trade)
            self.dm.save_trade(trade)

        elif retry_counter >= 0 and retry_counter < max_retries:
            self.unhandled_trade_callbacks.append((retry_counter + 1, trade))

        else:
            logger.warning(f"cannot map trade to order | trade: {trade}")
            self.dm.save_trade(trade)

    def on_positions_callback(self, positions: List[SF31Position]):
        self.dm.save_positions(positions)

    def sync(self):
        self.update_strategies()
        self.update_positions()
        if not self.contracts.update_dt or (
            is_trade_date() and not self.contracts.check_updated()
        ):
            self.update_contracts()

    def update_positions(self):
        positions = self.dm.get_positions()
        self.positions.update(positions)

    def update_strategies(self):
        strategies = self.dm.get_strategies()
        self.strategies.update(strategies)

    def update_contracts(self):
        contracts = self.dm.get_contracts()
        self.contracts.update(contracts)

    def update_snapshots(self):
        codes = self.positions.get_position_codes()
        snapshots = self.dm.get_quote_snapshots(codes)
        self.snapshots.update(snapshots)

    def reset(self):
        logger.info("reset")
        self.unhandled_orders.clear()
        self.order_callbacks.clear()
        self.unhandled_order_callbacks.clear()
        self.trade_callbacks.clear()
        self.unhandled_trade_callbacks.clear()

        xq_signal_dir = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_XQ_SIGNALS_DIR}"
        if os.path.exists(xq_signal_dir):
            for file in os.listdir(xq_signal_dir):
                os.remove(f"{xq_signal_dir}/{file}")

        order_path = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}/{Config.OBSERVER_ORDER_CALLBACK_FILE}"
        if os.path.exists(order_path):
            with open(order_path, "r+") as f:
                _ = f.truncate(0)

        trade_path = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}/{Config.OBSERVER_TRADE_CALLBACK_FILE}"
        if os.path.exists(trade_path):
            with open(trade_path, "r+") as f:
                _ = f.truncate(0)

        # position_path = f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}/{Config.OBSERVER_POSITION_CALLBACK_FILE}"
        # if os.path.exists(position_path):
        #     with open(position_path, "r+") as f:
        #         _ = f.truncate(0)

        sf31_order_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_SF31_ORDERS_DIR}"
        )
        for root, dir, files in os.walk(sf31_order_path):
            for file in files:
                if file.endswith(".log"):
                    file_path = f"{root}/{file}"
                    with open(file_path, "r+") as f:
                        _ = f.truncate(0)

        self.observer.reset_checkpoints()
        self.exit_handler.reset()

    def init_timer(self):
        self._prev_sync_ts = 0.0
        self._prev_snapshot_ts = 0.0
        self._next_reset_dt1 = get_next_schedule_time(Config.RESET_TIME1)
        self._next_reset_dt2 = get_next_schedule_time(Config.RESET_TIME2)

    def run_schedule_job(self):
        # schedule
        cur_dt = get_tpe_datetime()
        if cur_dt >= self._next_reset_dt1:
            self.reset()
            self.sync()
            self._next_reset_dt1 += dt.timedelta(days=1)

        if cur_dt >= self._next_reset_dt2:
            self.reset()
            self.sync()
            self._next_reset_dt2 += dt.timedelta(days=1)

        # interval
        if is_trade_date():
            if is_sync_time() and time.time() - self._prev_sync_ts > self.sync_interval:
                self.sync()
                self._prev_sync_ts = time.time()

            if (
                is_trade_time()
                and time.time() - self._prev_snapshot_ts > self.snapshot_interval
            ):
                self.update_snapshots()
                self.exit_handler.q_in.append((Event.Quote, self.snapshots))
                self._prev_snapshot_ts = time.time()

    def system_check(self) -> bool:
        if not is_signal_time():
            return False
        if not self.contracts.check_updated():
            if is_trade_time():
                logger.warning(
                    f"contracts not updated, previous update time: {self.contracts.update_dt}"
                )
            return False
        if not self.positions.check_updated():
            if is_trade_time():
                logger.warning(
                    f"positions not updated, previous update time: {self.positions.update_dt}"
                )
            return False
        if not self.strategies.check_updated():
            if is_trade_time():
                logger.warning(
                    f"strategies not updated, previous update time: {self.strategies.update_dt}"
                )
            return False
        return True

    def run(self):
        logger.info("Start Engine")
        self.init_timer()
        self.sync()
        self.observer.start()
        self.__thread_om.start()
        self.__thread_exit_handler.start()

        self.active = True
        while self.active:
            try:
                self.run_schedule_job()
                if not self.system_check():
                    time.sleep(10)
                    continue

                for _ in range(len(self.unhandled_order_callbacks)):
                    retry_counter, order = self.unhandled_order_callbacks.popleft()
                    self.on_order_callback(order, retry_counter=retry_counter)

                for _ in range(len(self.unhandled_trade_callbacks)):
                    retry_counter, trade = self.unhandled_trade_callbacks.popleft()
                    self.on_trade_callback(trade, retry_counter=retry_counter)

                while self.q_order_observer_out:
                    event, data = self.q_order_observer_out.popleft()
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
                    event, data = self.q_exit_handler_out.popleft()
                    if event == event.Signal:
                        self.on_signal(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

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
