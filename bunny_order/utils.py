import os
from loguru import logger
from datetime import datetime, timedelta
from typing import List
from functools import wraps
import time

from bunny_order.config import Config
from bunny_order.models import SF31Order


if not os.path.exists(Config.LOGURU_SINK_DIR):
    os.mkdir(Config.LOGURU_SINK_DIR)

logger.add(
    f"{Config.LOGURU_SINK_DIR}/{Config.LOGURU_SINK_FILE}",
    rotation="100MB",
    encoding="utf-8",
    enqueue=True,
    retention="30 days",
    level="INFO",
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


def send_sf31_orders(orders: List[SF31Order]):
    """
    N12,Stock,1684143670.093469,2882,ROD,B,1,43.10
    """
    pass


def _add_spread(price, count):
    _price = price
    for i in range(abs(count)):
        if count > 0:
            _price += _spread(_price, 1)
        elif count < 0:
            _price -= _spread(_price, -1)
    return _price


def _spread(price, up_or_down):
    if up_or_down == 1:
        if price < 10:
            return 0.01
        elif price >= 10 and price < 50:
            return 0.05
        elif price >= 50 and price < 100:
            return 0.1
        elif price >= 100 and price < 500:
            return 0.5
        elif price >= 500 and price < 1000:
            return 1
        elif price >= 1000:
            return 5

    elif up_or_down == -1:
        if price <= 10:
            return 0.01
        elif price > 10 and price <= 50:
            return 0.05
        elif price > 50 and price <= 100:
            return 0.1
        elif price > 100 and price <= 500:
            return 0.5
        elif price > 500 and price <= 1000:
            return 1
        elif price > 1000:
            return 5


def _spread_cnt(lower_price, upper_price):
    _price = lower_price
    if lower_price == upper_price:
        return 0
    else:
        for i in range(5000):
            _price += _spread(_price, 1)
            if _price > upper_price:
                return i - 1
