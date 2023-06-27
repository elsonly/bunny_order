import os
import sys
from loguru import logger
from datetime import datetime, timedelta
import datetime as dt
import threading
from functools import wraps
from decimal import Decimal, ROUND_HALF_UP
import json
import uuid

from bunny_order.config import Config


if not os.path.exists(Config.LOGURU_SINK_DIR):
    os.mkdir(Config.LOGURU_SINK_DIR)

logger.remove()
logger.add(sys.stdout, level=Config.LOGURU_LOG_LEVEL)
logger.add(
    f"{Config.LOGURU_SINK_DIR}/{Config.LOGURU_SINK_FILE}",
    rotation="100MB",
    encoding="utf-8",
    enqueue=True,
    retention="30 days",
    level=Config.LOGURU_LOG_LEVEL,
)


def get_tpe_datetime() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)


def event_wrapper(func):
    @wraps(func)
    def inner(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.exception(e)

    return inner


def adjust_price_for_tick_unit(price: Decimal) -> Decimal:
    if price < 0:
        raise Exception(f"Invalid price: {price}")
    elif price < 10:
        tick_unit = Decimal("0.01")
    elif price < 50:
        tick_unit = Decimal("0.05")
    elif price < 100:
        tick_unit = Decimal("0.1")
    elif price < 500:
        tick_unit = Decimal("0.5")
    elif price < 1000:
        tick_unit = Decimal("1")
    else:
        tick_unit = Decimal("5")

    adj_price = (
        (price / tick_unit).quantize(Decimal("1."), ROUND_HALF_UP) * tick_unit
    ).quantize(Decimal(".00"), ROUND_HALF_UP)
    return adj_price


def get_signal_id(digits: int = 16) -> str:
    return uuid.uuid4().hex[:digits]


def dump_checkpoints(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, indent=4, ensure_ascii=False))


def load_checkpoints(path: str) -> dict:
    data = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    return data


def is_signal_time() -> bool:
    return Config.DEBUG or (
        get_tpe_datetime().time() >= Config.SIGNAL_START_TIME
        and get_tpe_datetime().time() < Config.SIGNAL_END_TIME
    )


def is_before_market_time() -> bool:
    return Config.DEBUG or (
        get_tpe_datetime().time() >= Config.BEFORE_MARKET_START_TIME
        and get_tpe_datetime().time() < Config.BEFORE_MARKET_END_TIME
    )


def is_sync_time() -> bool:
    return Config.DEBUG or (
        get_tpe_datetime().time() >= Config.SYNC_START_TIME
        and get_tpe_datetime().time() <= Config.SYNC_END_TIME
    )


def is_trade_time() -> bool:
    return Config.DEBUG or (
        get_tpe_datetime().time() >= Config.TRADE_START_TIME
        and get_tpe_datetime().time() <= Config.TRADE_END_TIME
    )


def is_week_date() -> bool:
    return Config.DEBUG or (get_tpe_datetime().weekday() < 5)


def get_next_schedule_time(dtime: dt.time) -> dt.datetime:
    dt_ = dt.datetime.combine(get_tpe_datetime().date(), dtime)

    if get_tpe_datetime() >= dt_:
        next_dt = dt_ + dt.timedelta(days=1)
    else:
        next_dt = dt_

    return next_dt


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self):
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock."""
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        """Acquire a write lock. Blocks until there are no
        acquired read or write locks."""
        self._read_ready.acquire()
        counter = 0
        while self._readers > 0:
            self._read_ready.wait(10)
            counter += 1
            if counter > 5:
                logger.error("lock not release")

    def release_write(self):
        """Release a write lock."""
        self._read_ready.release()
