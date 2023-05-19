from typing import Optional
from pydantic import BaseModel
import datetime as dt
from enum import Enum
from decimal import Decimal

class SF31SecurityType(str, Enum):
    Stock = "Stock"


class OrderType(str, Enum):
    ROD = "ROD"
    IOC = "IOC"
    FOK = "FOK"


class Action(str, Enum):
    Buy = "B"
    Sell = "S"


class SecurityType(str, Enum):
    Stock = "S"
    Futures = "F"
    Option = "O"


class Strategy(BaseModel):
    id: int
    name: str
    add_date: Optional[dt.date]
    status: bool
    leverage_ratio: Optional[float]
    expected_mdd: Optional[float]
    expected_daily_return: Optional[float]
    holding_period: Optional[int]
    stop_loss: Optional[float]
    stop_profit: Optional[float]
    order_low_ratio: Optional[float]


class XQSignal(BaseModel):
    id: str
    sdate: dt.date
    stime: dt.time
    strategy_id: int
    security_type: SF31SecurityType
    code: str
    order_type: OrderType
    action: Action
    quantity: int
    price: Decimal


class SF31Order(BaseModel):
    signal_id: str
    sfdate: dt.date
    sftime: dt.time
    strategy_id: int
    security_type: SF31SecurityType
    code: str
    order_type: OrderType
    action: Action
    quantity: int
    price: Decimal
    order_id: str = ""


class Order(BaseModel):
    trader_id: str
    strategy: int
    order_id: str
    security_type: SecurityType
    order_date: dt.date
    order_time: dt.time
    code: str
    action: Action
    order_price: Decimal
    order_qty: int
    order_type: OrderType
    status: str
    msg: str = ""


class Trade(BaseModel):
    trader_id: str
    strategy: int
    order_id: str
    order_type: OrderType
    seqno: str
    security_type: SecurityType
    trade_date: dt.date
    trade_time: dt.time
    code: str
    action: Action
    price: Decimal
    qty: int


class Position(BaseModel):
    trader_id: str
