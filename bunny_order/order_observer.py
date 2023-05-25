import os
import re
import json
from typing import Dict, Tuple, DefaultDict, List, Deque
from collections import defaultdict
import pandas as pd
from watchdog.observers.polling import PollingObserver
from watchdog.events import (
    FileCreatedEvent,
    FileModifiedEvent,
    FileDeletedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
)
import datetime as dt


from bunny_order.config import Config
from bunny_order.utils import logger, get_tpe_datetime, event_wrapper
from bunny_order.models import (
    XQSignal,
    Strategy,
    SF31SecurityType,
    OrderType,
    Action,
    Order,
    SecurityType,
    Trade,
)


class FileEventHandler(FileSystemEventHandler):
    def on_moved(self, event: FileMovedEvent):
        if event.is_directory:
            logger.info(
                "directory moved from {0} to {1}".format(
                    event.src_path, event.dest_path
                )
            )
        else:
            logger.info(
                "file moved from {0} to {1}".format(event.src_path, event.dest_path)
            )

    def on_created(self, event: FileCreatedEvent):
        if event.is_directory:
            logger.info("directory created:{0}".format(event.src_path))
        else:
            logger.info("file created:{0}".format(event.src_path))

    def on_deleted(self, event: FileDeletedEvent):
        if event.is_directory:
            logger.info("directory deleted:{0}".format(event.src_path))
        else:
            logger.info("file deleted:{0}".format(event.src_path))

    def on_modified(self, event: FileModifiedEvent):
        if event.is_directory:
            logger.info("directory modified:{0}".format(event.src_path))
        else:
            logger.info("file modified:{0}".format(event.src_path))


class XQSignalEventHandler(FileEventHandler):
    def __init__(
        self,
        strategies: Dict[str, Strategy],
        q_signals: Deque[XQSignal],
    ):
        super().__init__()
        self.strategies = strategies
        self.q_signals = q_signals
        self.pattern = re.compile(r"(\d{8}_[^\\/]+\.log)$")
        self.prev_signal_dt = get_tpe_datetime()
        self.signal_counter = 1
        self.id_prefix = "A"
        self.checkpoints: DefaultDict[str, int] = defaultdict(int)
        self.checkpoints_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_XQ_SIGNALS_DIR}.json"
        )
        self.load_checkpoints()

    @event_wrapper
    def on_created(self, event: FileCreatedEvent):
        if event.is_directory:
            logger.info("directory created:{0}".format(event.src_path))
        else:
            logger.info("file created:{0}".format(event.src_path))
            date_, strategy = self.parse_file(event.src_path)
            if date_ == "" or strategy == "":
                return
            with open(event.src_path, "r", encoding="utf-8") as f:
                data = [x.split() for x in f.readlines()]

            self.on_signals(date_, strategy, data)
            self.checkpoints[strategy] = len(data)
            self.dump_checkpoints()

    @event_wrapper
    def on_modified(self, event: FileModifiedEvent):
        if event.is_directory:
            logger.info("directory modified:{0}".format(event.src_path))
        else:
            logger.info("file modified:{0}".format(event.src_path))
            date_, strategy = self.parse_file(event.src_path)
            if date_ == "" or strategy == "":
                return
            with open(event.src_path, "r", encoding="utf-8") as f:
                data = [x.split() for x in f.readlines()]

            if self.checkpoints[strategy] < len(data):
                self.on_signals(date_, strategy, data[self.checkpoints[strategy] :])
                self.checkpoints[strategy] = len(data)
                self.dump_checkpoints()

    def dump_checkpoints(self):
        with open(self.checkpoints_path, "w", encoding="utf8") as f:
            json.dump(self.checkpoints, f, indent=4, ensure_ascii=False)

    def load_checkpoints(self):
        if os.path.exists(self.checkpoints_path):
            with open(self.checkpoints_path, "r", encoding="utf8") as f:
                data = json.load(f)
            self.checkpoints.update(data)

    def reset_checkpoints(self):
        self.checkpoints.clear()
        self.dump_checkpoints()

    def get_signal_id(self) -> str:
        """
        return (str): signal id
            ex: 'A01', 'Z99'
        """
        now = get_tpe_datetime()
        if self.prev_signal_dt.date() < now.date():
            self.reset_counter()
        self.signal_counter = self.signal_counter % 100
        if self.signal_counter == 0:
            if self.id_prefix != "Z":
                self.signal_counter = 1
                self.id_prefix = chr(ord(self.id_prefix) + 1)
            else:
                self.id_prefix = "A"
        self.prev_signal_dt = now
        return f"{self.id_prefix}{str(self.signal_counter).rjust(2, '0')}"

    def reset_counter(self):
        self.signal_counter = 1
        self.id_prefix = "A"

    def parse_file(self, src_path: str) -> Tuple[str, str]:
        """
        src_path (str):
            example: './signals/xq_signals\\20230515_法說會前主力蠢蠢欲動.log'
        """
        match = re.search(self.pattern, src_path)
        if match is None:
            return "", ""
        parsed_path = match.group(1).replace(".log", "")
        date_, strategy = parsed_path.split("_")
        return date_, strategy

    def convert_to_signals(
        self, date: str, strategy: str, data: list
    ) -> List[XQSignal]:
        """
        date (str): %Y%m%d
            ex: '20230515'
        strategy (str): strategy name
        data (list):
            ex: ["173749 2882.TW ROD B 20 47.65"]
        """
        signals = []
        for x in data:
            if len(x) < 6:
                logger.debug(f"invalid data: {x}")
                continue
            n_hour = len(x[0])
            stime = dt.time(
                hour=int(x[0][: n_hour - 4]),
                minute=int(x[0][n_hour - 4 : n_hour - 2]),
                second=int(x[0][n_hour - 2 : n_hour]),
            )
            signal = XQSignal(
                id=self.get_signal_id(),
                sdate=pd.to_datetime(date).date(),
                stime=stime,
                strategy_id=self.strategies[strategy].id,
                security_type=SF31SecurityType.Stock,
                code=x[1].split(".")[0],
                order_type=OrderType(x[2]),
                action=Action(x[3]),
                quantity=int(x[4]),
                price=float(x[5]),
            )

            self.signal_counter += 1
            signals.append(signal)

        return signals

    def on_signals(self, date: str, strategy: str, data: List[str]):
        """
        date (str): %Y%m%d
            ex: '20230515'
        strategy (str): strategy name
        data (list):
            ex: ["173749 2882.TW ROD B 20 47.65"]
        """
        logger.debug(f"date: {date}, strategy: {strategy}, data: {data}")
        if strategy not in self.strategies:
            return
        signals = self.convert_to_signals(date, strategy, data)
        logger.debug(f"signals: {signals}")
        for signal in signals:
            self.q_signals.append(signal)


class OrderCallbackEventHandler(FileEventHandler):
    def __init__(
        self,
        q_orders: Deque[Order],
        q_trades: Deque[Trade],
    ):
        super().__init__()
        self.q_orders = q_orders
        self.q_trades = q_trades
        self.checkpoints: DefaultDict[str, int] = defaultdict(int)
        self.checkpoints_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}.json"
        )
        self.load_checkpoints()

    @event_wrapper
    def on_created(self, event: FileCreatedEvent):
        if event.is_directory:
            logger.info("directory created:{0}".format(event.src_path))
        else:
            logger.info("file created:{0}".format(event.src_path))

            with open(event.src_path, "r", encoding="utf-8") as f:
                data = [x.strip().split(",") for x in f.readlines()]

            if event.src_path.endswith(Config.OBSERVER_ORDER_CALLBACK_FILE):
                self.on_orders(data)
                self.checkpoints["orders"] = len(data)
                self.dump_checkpoints()

            elif event.src_path.endswith(Config.OBSERVER_TRADE_CALLBACK_FILE):
                self.on_trades(data)
                self.checkpoints["trades"] = len(data)
                self.dump_checkpoints()

            elif event.src_path.endswith(Config.OBSERVER_POSITION_CALLBACK_FILE):
                self.on_positions(data)
                self.checkpoints["positions"] = len(data)
                self.dump_checkpoints()

    @event_wrapper
    def on_modified(self, event: FileModifiedEvent):
        if event.is_directory:
            logger.info("directory modified:{0}".format(event.src_path))
        else:
            logger.info("file modified:{0}".format(event.src_path))

            with open(event.src_path, "r", encoding="utf-8") as f:
                data = [x.strip().split(",") for x in f.readlines()]

            if event.src_path.endswith(Config.OBSERVER_ORDER_CALLBACK_FILE):
                if self.checkpoints["orders"] < len(data):
                    self.on_orders(data[self.checkpoints["orders"] :])
                    self.checkpoints["orders"] = len(data)
                    self.dump_checkpoints()

            elif event.src_path.endswith(Config.OBSERVER_TRADE_CALLBACK_FILE):
                if self.checkpoints["trades"] < len(data):
                    self.on_trades(data[self.checkpoints["trades"] :])
                    self.checkpoints["trades"] = len(data)
                    self.dump_checkpoints()

            elif event.src_path.endswith(Config.OBSERVER_POSITION_CALLBACK_FILE):
                if self.checkpoints["positions"] < len(data):
                    self.on_positions(data[self.checkpoints["positions"] :])
                    self.checkpoints["positions"] = len(data)
                    self.dump_checkpoints()

    def dump_checkpoints(self):
        with open(self.checkpoints_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(self.checkpoints, indent=4))

    def load_checkpoints(self):
        if os.path.exists(self.checkpoints_path):
            with open(self.checkpoints_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.checkpoints.update(data)

    def reset_checkpoints(self):
        self.checkpoints.clear()
        self.dump_checkpoints()

    def on_orders(self, data: List[str]):
        """
        data (list):
            ex: [
                    '025,00000,現股,085004,8426,ROD,Buy,1,69.9,特定證券管制交易－類別錯誤',
                    '025,W003V,現股,085004,8446,ROD,Buy,1,113.5,'
                ]
        """
        for raw_order in data:
            n_hour = len(raw_order[3])
            order_time = dt.time(
                hour=int(raw_order[3][: n_hour - 4]),
                minute=int(raw_order[3][n_hour - 4 : n_hour - 2]),
                second=int(raw_order[3][n_hour - 2 : n_hour]),
            )
            order = Order(
                trader_id=raw_order[0],
                strategy=7,  # temp strategy id
                order_id=raw_order[1],
                security_type=SecurityType.Stock
                if raw_order[2] == "現股"
                else SecurityType.Futures,
                order_date=get_tpe_datetime().date(),
                order_time=order_time,
                code=raw_order[4],
                action=Action.Buy if raw_order[6] == "Buy" else Action.Sell,
                order_price=raw_order[8],
                order_qty=raw_order[7],
                order_type=OrderType(raw_order[5]),
                status="New" if raw_order[9] == "" else "Failed",
                msg=raw_order[9],
            )
            self.q_orders.append(order)

    def on_trades(self, data: List[str]):
        """
        data (list):
            ex: [
                    '025,W003O,現股,090008,8048,ROD,Buy,1,49.45,',
                    '025,W003U,現股,090009,8446,ROD,Buy,1,115,'
                ]
        """
        for raw_trade in data:
            n_hour = len(raw_trade[3])
            trade_time = dt.time(
                hour=int(raw_trade[3][: n_hour - 4]),
                minute=int(raw_trade[3][n_hour - 4 : n_hour - 2]),
                second=int(raw_trade[3][n_hour - 2 : n_hour]),
            )
            trade = Trade(
                trader_id=raw_trade[0],
                strategy=7,  # temp strategy id
                order_id=raw_trade[1],
                security_type=SecurityType.Stock
                if raw_trade[2] == "現股"
                else SecurityType.Futures,
                trade_date=get_tpe_datetime().date(),
                trade_time=trade_time,
                code=raw_trade[4],
                order_type=OrderType(raw_trade[5]),
                action=Action.Buy if raw_trade[6] == "Buy" else Action.Sell,
                qty=raw_trade[7],
                price=raw_trade[8],
                # TODO: seqno
                seqno="",
            )
            self.q_trades.append(trade)

    def on_positions(self, data: List[str]):
        logger.info(data)


class OrderObserver:
    def __init__(
        self,
        strategies: Dict[str, Strategy],
        q_signals: Deque[XQSignal],
        q_orders: Deque[Order],
        q_trades: Deque[Trade],
    ):
        self.observer = PollingObserver()
        self.observer.setDaemon(True)

        if not os.path.exists(Config.OBSERVER_BASE_PATH):
            os.mkdir(Config.OBSERVER_BASE_PATH)

        # XQSignalEvent
        xq_signals_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_XQ_SIGNALS_DIR}"
        )
        if not os.path.exists(xq_signals_path):
            os.mkdir(xq_signals_path)

        logger.info(f"listen to folder: {xq_signals_path}")
        self.xq_signal_event_handler = XQSignalEventHandler(
            strategies, q_signals=q_signals
        )
        self.observer.schedule(self.xq_signal_event_handler, xq_signals_path, False)

        # OrderCallbackEvent
        order_callback_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}"
        )
        if not os.path.exists(order_callback_path):
            os.mkdir(order_callback_path)

        logger.info(f"listen to folder: {order_callback_path}")
        self.order_callback_event_handler = OrderCallbackEventHandler(
            q_orders, q_trades
        )
        self.observer.schedule(
            self.order_callback_event_handler,
            order_callback_path,
            False,
        )

    def reset_checkpoints(self):
        self.xq_signal_event_handler.reset_checkpoints()
        self.order_callback_event_handler.reset_checkpoints()

    def _del__(self):
        self.observer.stop()

    def start(self):
        self.observer.start()
