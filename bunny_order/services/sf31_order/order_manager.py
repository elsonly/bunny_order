import os
import uuid
from pathlib import Path
import time
import json
import datetime as dt
from typing import Dict, Tuple, DefaultDict, List, Callable, Deque
from collections import defaultdict, deque
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import (
    FileCreatedEvent,
    FileModifiedEvent,
    FileDeletedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
)
import datetime as dt


from bunny_order.database.data_manager import DataManager
from bunny_order.config import Config
from bunny_order.utils import logger, get_tpe_datetime, event_wrapper
from bunny_order.models import (
    Strategy,
    SF31Order,
    OrderType,
    Action,
    Order,
    SecurityType,
    Trade,
)
from bunny_order.common import Strategies


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


class OrderEventHandler(FileEventHandler):
    def __init__(self, strategies: Strategies, listen_path: str):
        super().__init__()
        self.strategies = strategies
        self.listen_path = listen_path
        self.order_callback_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_ORDER_CALLBACK_DIR}"
        )
        self.checkpoints: DefaultDict[str, DefaultDict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self.checkpoints_path = (
            f"{Config.CHECKPOINTS_DIR}/{Config.OBSERVER_SF31_ORDERS_DIR}.json"
        )
        self.load_checkpoints()

    @event_wrapper
    def on_created(self, event: FileCreatedEvent):
        src_path = Path(event.src_path)
        if event.is_directory:
            logger.info("directory created:{0}".format(src_path))
        else:
            logger.info("file created:{0}".format(src_path))

            with open(src_path, "r", encoding="utf-8") as f:
                data = [x.strip().split(",") for x in f.readlines()]

            action, strategy = self.parse_path(src_path)
            if self.checkpoints[strategy][action] == len(data):
                return
            self.on_sf31_orders(strategy, data)
            self.checkpoints[strategy][action] = len(data)
            self.dump_checkpoints()

    @event_wrapper
    def on_modified(self, event: FileModifiedEvent):
        src_path = Path(event.src_path)
        if event.is_directory:
            logger.info("directory modified:{0}".format(src_path))
        else:
            logger.info("file modified:{0}".format(src_path))

            with open(src_path, "r", encoding="utf-8") as f:
                data = [x.strip().split(",") for x in f.readlines()]

            action, strategy = self.parse_path(src_path)
            if self.checkpoints[strategy][action] < len(data):
                self.on_sf31_orders(
                    strategy, data[self.checkpoints[strategy][action] :]
                )
                self.checkpoints[strategy][action] = len(data)
                self.dump_checkpoints()

    def parse_path(self, src_path: Path) -> Tuple[str, str]:
        """
        src_path (Path):
            example: ./signals/sf31_orders/處置股10日多/Buy.log
        """
        action = src_path.name.replace(".log", "")
        strategy = src_path.parent.name
        return action, strategy

    def dump_checkpoints(self):
        with open(self.checkpoints_path, "w", encoding="utf-8") as f:
            json.dump(self.checkpoints, f, indent=4, ensure_ascii=False)

    def load_checkpoints(self):
        if os.path.exists(self.checkpoints_path):
            with open(self.checkpoints_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.checkpoints.update(data)

    def get_order_id(self):
        return uuid.uuid4().hex[:5]

    def get_seqno(self):
        return uuid.uuid4().hex[:12]

    def on_sf31_orders(self, strategy: str, data: List[str]):
        """
        data (list):
            ex: ['A15,Stock,1684852278.968826,2882,ROD,B,10,47.65']
        """
        strategy_id = self.strategies.get_id(strategy)
        if strategy_id == 0:
            return
        for raw_order in data:
            if raw_order[1] == "Stock":
                security_type = SecurityType.Stock
            else:
                raise Exception(f"unhandled security type: {raw_order[1]}")
            dt_ = dt.datetime.fromtimestamp(float(raw_order[2]))
            order = SF31Order(
                signal_id=raw_order[0],
                sfdate=dt_.date(),
                sftime=dt_.time(),
                strategy_id=strategy_id,
                security_type=security_type,
                code=raw_order[3],
                order_type=OrderType(raw_order[4]),
                action=Action(raw_order[5]),
                quantity=raw_order[6],
                price=raw_order[7],
            )
            order_id = self.get_order_id()
            self.to_order(order_id, order)
            self.to_trade(order_id, order)

    def to_order(self, order_id: str, order: SF31Order):
        """
        025,00000,現股,085004,8426,ROD,Buy,1,63.4,特定證券管制交易－類別錯誤,2023/05/26
        """
        logger.info(order)
        if order.security_type == SecurityType.Stock:
            security_type = "現股"
        else:
            raise Exception(f"Unhandle Secutiry Type: {order.security_type}")

        if order.action == Action.Buy:
            action = "Buy"
        else:
            action = "Sell"

        with open(
            f"{self.order_callback_path}\{Config.OBSERVER_ORDER_CALLBACK_FILE}",
            "a",
            encoding="utf8",
        ) as f:
            f.write(
                (
                    f"025,{order_id},{security_type},{order.sftime.strftime('%H%M%S')},{order.code},"
                    f"{order.order_type},{action},{order.quantity},{order.price},,{order.sfdate.strftime('%Y/%m/%d')}\n"
                )
            )

    def to_trade(self, order_id: str, order: SF31Order):
        """
        025,W003U,現股,090009,8446,ROD,Buy,1,115,,2023/05/26,100000038840
        """
        logger.info(order)
        if order.security_type == SecurityType.Stock:
            security_type = "現股"
        else:
            raise Exception(f"Unhandle Secutiry Type: {order.security_type}")

        if order.action == Action.Buy:
            action = "Buy"
        else:
            action = "Sell"

        with open(
            f"{self.order_callback_path}\{Config.OBSERVER_TRADE_CALLBACK_FILE}",
            "a",
            encoding="utf8",
        ) as f:
            f.write(
                (
                    f"025,{order_id},{security_type},{order.sftime.strftime('%H%M%S')},{order.code},"
                    f"{order.order_type},{action},{order.quantity},{order.price},"
                    f",{order.sfdate.strftime('%Y/%m/%d')},{self.get_seqno()}\n"
                )
            )


class OrderManager:
    def __init__(
        self,
    ):
        self.dm = DataManager()
        self.strategies = Strategies()
        self.strategies.update(self.dm.get_strategies())
        self.observer = Observer()
        self.observer.setDaemon(True)
        if not os.path.exists(Config.OBSERVER_BASE_PATH):
            os.mkdir(Config.OBSERVER_BASE_PATH)

        sf31_orders_path = (
            f"{Config.OBSERVER_BASE_PATH}/{Config.OBSERVER_SF31_ORDERS_DIR}"
        )
        if not os.path.exists(sf31_orders_path):
            os.mkdir(sf31_orders_path)

        self.observer.schedule(
            OrderEventHandler(self.strategies, sf31_orders_path), sf31_orders_path, True
        )

    def _del__(self):
        self.observer.stop()

    def run(self):
        self.observer.start()
        logger.info("start")
        prev_update_ts = 0
        while True:
            if time.time() - prev_update_ts > 10:
                self.strategies.update(self.dm.get_strategies())
                prev_update_ts = time.time()
            time.sleep(1)
