import datetime as dt
import os
from decimal import Decimal
import time
from typing import Dict, List, Deque, Tuple, Union
import threading
from collections import deque, defaultdict

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
    get_seqno,
    get_order_id,
)
from bunny_order.common import Strategies, Contracts, TradingDates
from bunny_order.config import Config


class SignalCollector:
    def __init__(self, dm: DataManager, contracts: Contracts):
        self.dm = dm
        self.contracts = contracts
        self.collector: Dict[str, Dict[Action, List[Signal]]] = {}
        self.__last_ts = 0
        self._offsetting_signals: List[Signal] = []
        self._signals: List[Signal] = []

    def on_signal(self, signal: Signal):
        logger.info(signal)
        if signal.code not in self.collector:
            self.collector[signal.code] = {}
        if signal.action not in self.collector[signal.code]:
            self.collector[signal.code][signal.action] = []
        self.collector[signal.code][signal.action].append(signal)
        self.__last_ts = time.time()

    def _offset_signals(self, signals: Dict[Action, List[Signal]]):
        if Action.Buy not in signals and Action.Sell not in signals:
            return
        elif Action.Buy in signals and Action.Sell not in signals:
            self._signals.extend(signals[Action.Buy])
            return
        elif Action.Buy not in signals and Action.Sell in signals:
            self._signals.extend(signals[Action.Sell])
            return
        buy_signals = signals[Action.Buy]
        sell_signals = signals[Action.Sell]
        if len(buy_signals) == 0 or len(sell_signals) == 0:
            self._signals.extend(buy_signals)
            self._signals.extend(sell_signals)
            return

        for s_signal in sell_signals:
            if s_signal.quantity == 0:
                continue
            for b_signal in buy_signals:
                if b_signal.quantity == 0:
                    continue
                offset_qty = min(b_signal.quantity, s_signal.quantity)
                offset_s_signal = s_signal.copy(deep=True)
                offset_s_signal.quantity = offset_qty
                offset_b_signal = b_signal.copy(deep=True)
                offset_b_signal.quantity = offset_qty
                s_signal.quantity -= offset_qty
                b_signal.quantity -= offset_qty
                self._offsetting_signals.append(offset_b_signal)
                self._offsetting_signals.append(offset_s_signal)

                if s_signal.quantity == 0:
                    break

        self._signals.extend(buy_signals)
        self._signals.extend(sell_signals)

    def check_signals(self) -> bool:
        if get_tpe_datetime().time() < dt.time(hour=9, minute=0, second=0):
            offset_interval = 60
        elif Config.DEBUG:
            offset_interval = 5
        else:
            offset_interval = 0

        if time.time() - self.__last_ts < offset_interval:
            return False

        for code in list(self.collector):
            signals = self.collector.pop(code)
            self._offset_signals(signals)

        if len(self._signals) > 0:
            return True
        elif len(self._offsetting_signals) > 0:
            return True
        else:
            return False

    def execute_offsetting_signals(self):
        for _ in range(len(self._offsetting_signals)):
            signal = self._offsetting_signals.pop()
            self._place_mock_order(signal)

    def get_signals(self) -> List[Signal]:
        return [self._signals.pop() for _ in range(len(self._signals))]

    def _place_mock_order(self, signal: Signal):
        logger.info(signal)
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
        self.dm.save_sf31_order(order)
        order_cb = self._mock_order_callback(order)
        self.dm.save_order(order_cb)
        order.order_id = order_cb.order_id
        self.dm.update_sf31_order(order)
        trade_cb = self._mock_trade_callback(order_cb)
        self.dm.save_trade(trade_cb)

    def _mock_order_callback(self, order: SF31Order) -> Order:
        order_time = (
            order.sftime
            if order.sftime >= dt.time(hour=9, minute=0, second=0)
            else dt.time(hour=9, minute=0, second=0)
        )
        return Order(
            trader_id="000",
            strategy=order.strategy_id,
            order_id=get_order_id(),
            security_type=order.security_type,
            order_date=order.sfdate,
            order_time=order_time,
            code=order.code,
            action=order.action,
            order_price=order.price,
            order_qty=order.quantity,
            order_type=order.order_type,
            price_type=order.price_type,
            status="New",
            msg="",
        )

    def _mock_trade_callback(self, order_cb: Order) -> Trade:
        return Trade(
            trader_id=order_cb.trader_id,
            strategy=order_cb.strategy,
            order_id=order_cb.order_id,
            order_type=order_cb.order_type,
            seqno=get_seqno(),
            security_type=order_cb.security_type,
            trade_date=order_cb.order_date,
            trade_time=order_cb.order_time,
            code=order_cb.code,
            action=order_cb.action,
            # TODO: deal price
            price=self.contracts.get_contract(order_cb.code).reference,
            qty=order_cb.order_qty,
        )


class OrderManager:
    def __init__(
        self,
        strategies: Strategies,
        contracts: Contracts,
        trading_dates: TradingDates,
        unhandled_orders: Deque[SF31Order] = deque(),
        q_in: Deque[Tuple[Event, Union[Signal, Order, Trade]]] = deque(),
        active_event: threading.Event = threading.Event(),
    ):
        self.q_in = q_in
        self.strategies = strategies
        self.contracts = contracts
        self.trading_dates = trading_dates
        self.unhandled_orders = unhandled_orders
        self.dm = DataManager()
        self.s31_orders_dir = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_SF31_ORDERS_DIR}"
        )
        self.pause_order = False
        self.active_event = active_event
        self.pending_signals: Deque[Signal] = deque()
        self.signal_collector = SignalCollector(dm=self.dm, contracts=self.contracts)

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

    def execute_orders_half_open_half_order_low_ratio(self, signal: Signal):
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

    def execute_limit_order(self, signal: Signal):
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
            if signal.action == Action.Buy:
                self.execute_orders_half_open_half_order_low_ratio(signal)
            else:
                self.execute_limit_order(signal)
        elif signal.source == SignalSource.ExitHandler:
            self.execute_limit_order(signal)
        else:
            raise Exception(f"invalid signal source: {signal.source}")

    def on_order_callback(self, order: Order):
        logger.info(order)

    def on_trade_callback(self, trade: Trade):
        logger.info(trade)

    def system_check(self) -> bool:
        if not is_trade_time():
            return False
        if not self.trading_dates.check_updated():
            if is_trade_time():
                logger.warning(
                    f"trading_dates not updated, previous update time: {self.contracts.update_dt}"
                )
            return False
        if not self.trading_dates.is_trading_date():
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
                        self.signal_collector.on_signal(data)
                    elif event == Event.OrderCallback:
                        self.on_order_callback(data)
                    elif event == Event.TradeCallback:
                        self.on_trade_callback(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

                if self.signal_collector.check_signals():
                    for signal in self.signal_collector.get_signals():
                        self.on_signal(signal)
                    self.signal_collector.execute_offsetting_signals()

            except Exception as e:
                logger.exception(e)

            time.sleep(0.01)
        logger.info("Shutdown Order Manager")
