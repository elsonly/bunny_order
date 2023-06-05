import os
import re
import json
from typing import Dict, Tuple, DefaultDict, List, Deque, Union
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
from bunny_order.utils import (
    logger,
    event_wrapper,
    get_signal_id,
    dump_checkpoints,
    load_checkpoints,
)
from bunny_order.models import (
    Signal,
    Strategy,
    OrderType,
    Action,
    Order,
    SecurityType,
    Trade,
    SF31Position,
    Event,
    PriceType,
    SignalSource,
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
        strategies: Dict[int, Strategy],
        q_out: Deque[Tuple[Event, Union[Signal, Order, Trade, List[SF31Position]]]],
    ):
        super().__init__()
        self.strategies = strategies
        self.q_out = q_out
        self.pattern = re.compile(r"(\d{8}_[^\\/]+\.log)$")
        self.checkpoints: DefaultDict[str, int] = defaultdict(int)
        self.checkpoints_path = (
            f"{Config.CHECKPOINTS_DIR}/{Config.OBSERVER_XQ_SIGNALS_DIR}.json"
        )
        self.checkpoints.update(load_checkpoints(self.checkpoints_path))

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
            dump_checkpoints(self.checkpoints_path, self.checkpoints)

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
                dump_checkpoints(self.checkpoints_path, self.checkpoints)

    def reset_checkpoints(self):
        self.checkpoints.clear()
        dump_checkpoints(self.checkpoints_path, self.checkpoints)

    def parse_file(self, src_path: str) -> Tuple[str, str]:
        """
        src_path (str):
            example: './signals/xq_signals\\20230515_法說會前主力蠢蠢欲動.log'
        """
        match = re.search(self.pattern, src_path)
        if match is None:
            return "", ""
        parsed_path = match.group(1).replace(".log", "")
        split_data = parsed_path.split("_")
        date_ = split_data[0]
        strategy = "_".join(split_data[1:])
        return date_, strategy

    def get_strategy_id(self, strategy_name: str) -> int:
        for _id, strategy in self.strategies.items():
            if strategy_name == strategy.name:
                return strategy.id
        return 0

    def convert_to_signals(self, date: str, strategy: str, data: list) -> List[Signal]:
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
            signal = Signal(
                id=get_signal_id(),
                source=SignalSource.XQ,
                sdate=pd.to_datetime(date).date(),
                stime=stime,
                strategy_id=self.get_strategy_id(strategy),
                security_type=SecurityType.Stock,
                code=x[1].split(".")[0],
                order_type=OrderType(x[2]),
                # price_type=PriceType.LMT,
                action=Action(x[3]),
                quantity=int(x[4]),
                price=float(x[5]),
            )

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
        logger.info(f"date: {date}, strategy: {strategy}, data: {data}")
        if self.get_strategy_id(strategy) == 0:
            return
        signals = self.convert_to_signals(date, strategy, data)
        logger.info(f"signals: {signals}")
        for signal in signals:
            self.q_out.append((Event.Signal, signal))


class OrderCallbackEventHandler(FileEventHandler):
    def __init__(
        self,
        q_out: Deque[Tuple[Event, Union[Signal, Order, Trade, List[SF31Position]]]],
    ):
        super().__init__()
        self.q_out = q_out
        self.checkpoints: DefaultDict[str, int] = defaultdict(int)
        self.checkpoints_path = (
            f"{Config.CHECKPOINTS_DIR}/{Config.OBSERVER_ORDER_CALLBACK_DIR}.json"
        )
        self.checkpoints.update(load_checkpoints(self.checkpoints_path))

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
                dump_checkpoints(self.checkpoints_path, self.checkpoints)

            elif event.src_path.endswith(Config.OBSERVER_TRADE_CALLBACK_FILE):
                self.on_trades(data)
                self.checkpoints["trades"] = len(data)
                dump_checkpoints(self.checkpoints_path, self.checkpoints)

            elif event.src_path.endswith(Config.OBSERVER_POSITION_CALLBACK_FILE):
                self.on_positions(data)
                self.checkpoints["positions"] = len(data)
                dump_checkpoints(self.checkpoints_path, self.checkpoints)

    @event_wrapper
    def on_modified(self, event: FileModifiedEvent):
        if event.is_directory:
            logger.info("directory modified:{0}".format(event.src_path))
        else:
            if event.src_path.endswith(Config.OBSERVER_POSITION_CALLBACK_FILE):
                logger.debug("file modified:{0}".format(event.src_path))
            else:
                logger.info("file modified:{0}".format(event.src_path))

            with open(event.src_path, "r", encoding="utf-8") as f:
                data = [x.strip().split(",") for x in f.readlines()]

            if event.src_path.endswith(Config.OBSERVER_ORDER_CALLBACK_FILE):
                if self.checkpoints["orders"] < len(data):
                    self.on_orders(data[self.checkpoints["orders"] :])
                    self.checkpoints["orders"] = len(data)
                    dump_checkpoints(self.checkpoints_path, self.checkpoints)

            elif event.src_path.endswith(Config.OBSERVER_TRADE_CALLBACK_FILE):
                if self.checkpoints["trades"] < len(data):
                    self.on_trades(data[self.checkpoints["trades"] :])
                    self.checkpoints["trades"] = len(data)
                    dump_checkpoints(self.checkpoints_path, self.checkpoints)

            elif event.src_path.endswith(Config.OBSERVER_POSITION_CALLBACK_FILE):
                if self.checkpoints["positions"] < len(data):
                    self.on_positions(data[self.checkpoints["positions"] :])
                    self.checkpoints["positions"] = len(data)
                    dump_checkpoints(self.checkpoints_path, self.checkpoints)

                # reset positions
                if self.checkpoints["positions"] > 2000:
                    with open(event.src_path, "r+") as f:
                        _ = f.truncate(0)
                    self.checkpoints["positions"] = 0
                    dump_checkpoints(self.checkpoints_path, self.checkpoints)

    def reset_checkpoints(self):
        self.checkpoints.clear()
        dump_checkpoints(self.checkpoints_path, self.checkpoints)

    def on_orders(self, data: List[List[str]]):
        """
        data (list):
            ex: [
                    '025,00000,現股,085004,8426,ROD,Buy,1,69.9,特定證券管制交易－類別錯誤,2023/05/26',
                    '025,W003t,現股,085004,3583,ROD,Sell,3,94.1,,2023/05/26'
                ]
        """
        for raw_order in data:
            if len(raw_order) > 11:
                # TODO:HOT FIX
                _date = raw_order.pop()
                _msg = raw_order.pop()
                _msg = raw_order.pop() + " " + _msg
                raw_order.append(_msg)
                raw_order.append(_date)
                
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
                order_date=pd.to_datetime(raw_order[10]).date(),
                order_time=order_time,
                code=raw_order[4],
                action=Action.Buy if raw_order[6] == "Buy" else Action.Sell,
                order_price=raw_order[8],
                # TODO: price type for mkt
                price_type=PriceType.LMT,
                order_qty=raw_order[7],
                order_type=OrderType(raw_order[5]),
                status="New" if raw_order[9] == "" else "Failed",
                msg=raw_order[9],
            )
            self.q_out.append((Event.OrderCallback, order))

    def on_trades(self, data: List[str]):
        """
        data (list):
            ex: [
                    '025,W003l,現股,090353,4129,ROD,Buy,1,62.4,,2023/05/26,100000038839',
                    '025,W003s,現股,090015,2353,ROD,Sell,1,30.75,,2023/05/26,200000045227'
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
                trade_date=pd.to_datetime(raw_trade[10]).date(),
                trade_time=trade_time,
                code=raw_trade[4],
                order_type=OrderType(raw_trade[5]),
                action=Action.Buy if raw_trade[6] == "Buy" else Action.Sell,
                qty=raw_trade[7],
                price=raw_trade[8],
                seqno=raw_trade[11],
            )
            self.q_out.append((Event.TradeCallback, trade))

    def on_positions(self, data: List[str]):
        """
        data (list):
            ex: [
                '025,100530,現股,6112,10000,62.6,0,99000.0,4000.0,0.158147',
                '025,100530,現股,8048,3000,49.4833,2847,5897.0,0.0,0.020546',
                '025,100530,現股,8446,2000,114.25,0,-6500,3000,-0.028446',
            ]
        """
        logger.debug(data)
        positions = []
        for raw_pos in data:
            if raw_pos[0].startswith('\x00'):
                continue
            n_hour = len(raw_pos[1])
            ptime = dt.time(
                hour=int(raw_pos[1][: n_hour - 4]),
                minute=int(raw_pos[1][n_hour - 4 : n_hour - 2]),
                second=int(raw_pos[1][n_hour - 2 : n_hour]),
            )
            position = SF31Position(
                trader_id=raw_pos[0],
                ptime=ptime,
                security_type=SecurityType.Stock
                if raw_pos[2] == "現股"
                else SecurityType.Futures,
                code=raw_pos[3],
                action=Action.Buy,
                shares=raw_pos[4],
                avg_price=raw_pos[5],
                closed_pnl=raw_pos[6],
                open_pnl=raw_pos[7],
                pnl_chg=raw_pos[8],
                cum_return=raw_pos[9],
            )
            positions.append(position)
        self.q_out.append((Event.PositionsCallback, positions))


class OrderObserver:
    def __init__(
        self,
        strategies: Dict[int, Strategy],
        q_out: Deque[Tuple[Event, Union[Signal, Order, Trade, List[SF31Position]]]],
    ):
        self.observer = PollingObserver()
        self.observer.setDaemon(True)
        self.q_out = q_out

        if not os.path.exists(Config.OBSERVER_BASE_PATH):
            os.mkdir(Config.OBSERVER_BASE_PATH)

        # SignalEvent
        xq_signals_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_XQ_SIGNALS_DIR}"
        )
        if not os.path.exists(xq_signals_path):
            os.mkdir(xq_signals_path)

        logger.info(f"listen to folder: {xq_signals_path}")
        self.xq_signal_event_handler = XQSignalEventHandler(strategies, q_out=q_out)
        self.observer.schedule(self.xq_signal_event_handler, xq_signals_path, False)

        # OrderCallbackEvent
        order_callback_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}"
        )
        if not os.path.exists(order_callback_path):
            os.mkdir(order_callback_path)

        logger.info(f"listen to folder: {order_callback_path}")
        self.order_callback_event_handler = OrderCallbackEventHandler(q_out)
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
