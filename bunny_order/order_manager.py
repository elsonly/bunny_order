import datetime as dt
import os
from decimal import Decimal
import time
from typing import Dict, List, Deque, Tuple, Union
import threading
from collections import deque

from bunny_order.models import (
    SignalSource,
    SF31Order,
    Action,
    Signal,
    Strategy,
    Event,
    Order,
    Trade,
    SecurityType,
    Contract,
)
from bunny_order.database.data_manager import DataManager
from bunny_order.utils import (
    logger,
    adjust_price_for_tick_unit,
    get_tpe_datetime,
    is_trade_time,
    is_trade_date,
)
from bunny_order.common import Strategies, Contracts
from bunny_order.config import Config


class OrderManager:
    def __init__(
        self,
        strategies: Strategies,
        contracts: Contracts,
        unhandled_orders: Deque[SF31Order] = deque(),
        q_in: Deque[Tuple[Event, Union[Signal, Order, Trade]]] = deque(),
        active_event: threading.Event = threading.Event(),
    ):
        self.q_in = q_in
        self.strategies = strategies
        self.contracts = contracts
        self.unhandled_orders = unhandled_orders
        self.dm = DataManager()
        self.s31_orders_dir = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_SF31_ORDERS_DIR}"
        )
        self.pause_order = False
        self.active_event = active_event
        self.pending_signals: Deque[Signal] = deque()

    def reset(self):
        self.unhandled_orders.clear()

    def place_order(self, order: SF31Order):
        """
        N12,Stock,1684143670.093469,2882,ROD,B,1,43.10
        """
        logger.info(order)
        self.unhandled_orders.append(order)

        strategy_name = self.strategies.get_strategy(order.strategy_id).name
        ts = dt.datetime.combine(order.sfdate, order.sftime).timestamp()
        if not os.path.exists(f"{self.s31_orders_dir}"):
            os.mkdir(f"{self.s31_orders_dir}")
        if not os.path.exists(f"{self.s31_orders_dir}/{strategy_name}"):
            os.mkdir(f"{self.s31_orders_dir}/{strategy_name}")

        if order.action == Action.Buy:
            path = f"{self.s31_orders_dir}/{strategy_name}/Buy.log"
        elif order.action == Action.Sell:
            path = f"{self.s31_orders_dir}/{strategy_name}/Sell.log"

        if order.security_type == SecurityType.Stock:
            security_type = "Stock"
        else:
            raise Exception(f"Invalid security_type {order.security_type}")

        order_string = (
            f"{order.signal_id},{security_type},{ts},{order.code},"
            f"{order.order_type},{order.action},{order.quantity},{order.price}\n"
        )
        with open(path, "a") as f:
            f.write(order_string)
        self.dm.save_sf31_order(order)

    def cancel_order(self, order: SF31Order):
        pass

    def price_order_low_ratio_adjustment(self, signal: Signal) -> Decimal:
        strategy = self.strategies.get_strategy(signal.strategy_id)
        contract = self.contracts.get_contract(signal.code)
        if strategy.order_low_ratio is not None:
            adj_prc_float = contract.reference * (
                1 + Decimal(strategy.order_low_ratio / 100)
            )
            adj_prc = adjust_price_for_tick_unit(adj_prc_float)
            return adj_prc
        else:
            return signal.price

    def excute_orders_half_open_half_order_low_ratio(self, signal: Signal):
        # split signal into 2 orders
        order1 = SF31Order(
            signal_id=signal.id,
            sfdate=signal.sdate,
            sftime=get_tpe_datetime().time(),
            strategy_id=signal.strategy_id,
            security_type=signal.security_type,
            code=signal.code,
            order_type=signal.order_type,
            price_type=signal.price_type,
            action=signal.action,
            quantity=signal.quantity - int(0.5 * signal.quantity),
            price=signal.price,
        )
        self.place_order(order1)

        order2 = SF31Order(
            signal_id=signal.id,
            sfdate=signal.sdate,
            sftime=get_tpe_datetime().time(),
            strategy_id=signal.strategy_id,
            security_type=signal.security_type,
            code=signal.code,
            order_type=signal.order_type,
            price_type=signal.price_type,
            action=signal.action,
            quantity=int(0.5 * signal.quantity),
            price=self.price_order_low_ratio_adjustment(signal),
        )
        self.place_order(order2)

    def excute_pre_market_orders(self, signal: Signal):
        if not is_trade_time():
            logger.warning(f"invalid trade time for signal: {signal}")
            return
        order = SF31Order(
            signal_id=signal.id,
            sfdate=signal.sdate,
            sftime=get_tpe_datetime().time(),
            strategy_id=signal.strategy_id,
            security_type=signal.security_type,
            code=signal.code,
            order_type=signal.order_type,
            price_type=signal.price_type,
            action=signal.action,
            quantity=signal.quantity,
            price=signal.price,
        )
        self.place_order(order)

    def on_signal(self, signal: Signal):
        logger.info(signal)
        if signal.source == SignalSource.XQ:
            self.excute_orders_half_open_half_order_low_ratio(signal)
        elif signal.source == SignalSource.ExitHandler:
            self.excute_pre_market_orders(signal)
        else:
            raise Exception(f"invalid signal source: {signal.source}")

    def on_order_callback(self, order: Order):
        logger.info(order)

    def on_trade_callback(self, trade: Trade):
        logger.info(trade)

    def system_check(self) -> bool:
        if not is_trade_date():
            return False
        if not self.contracts.check_updated():
            if is_trade_time():
                logger.warning(
                    f"contracts not updated, previous update time: {self.contracts.update_dt}"
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
        logger.info("Start Order Manager")
        while not self.active_event.isSet():
            try:
                if not self.system_check():
                    time.sleep(10)
                    continue

                if self.q_in:
                    event, data = self.q_in.popleft()
                    if event == Event.Signal:
                        if is_trade_time():
                            self.on_signal(data)
                        else:
                            self.pending_signals.append(data)
                    elif event == Event.OrderCallback:
                        self.on_order_callback(data)
                    elif event == Event.TradeCallback:
                        self.on_trade_callback(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

                while is_trade_time() and self.pending_signals:
                    signal = self.pending_signals.popleft()
                    self.on_signal(signal)

            except Exception as e:
                logger.exception(e)

            time.sleep(0.01)
        logger.info("Shutdown Order Manager")
