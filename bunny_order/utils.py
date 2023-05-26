import os
from loguru import logger
from datetime import datetime, timedelta
from functools import wraps
from decimal import Decimal, ROUND_HALF_UP

from bunny_order.config import Config


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


SIGNAL_COUNTER = 0


def get_signal_id() -> str:
    """
    return (str): signal id
        ex: '001', '999'
    """
    global SIGNAL_COUNTER
    SIGNAL_COUNTER += 1
    SIGNAL_COUNTER = SIGNAL_COUNTER % 1000
    signal_id = f"{str(SIGNAL_COUNTER).rjust(3, '0')}"
    return signal_id
