from typing import Optional
from pydantic import BaseModel
import datetime as dt
from enum import Enum
from decimal import Decimal


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


class SignalSource(str, Enum):
    XQ = "XQ"
    ExitHandler = "ExitHandler"


class PriceType(str, Enum):
    LMT = "LMT"
    MKT = "MKT"
    MOP = "MOP"


class ExitType(str, Enum):
    ExitByOutDate = "ExitByOutDate"
    ExitByDaysProfitLimit = "ExitByDaysProfitLimit"
    ExitByTakeProfit = "ExitByTakeProfit"
    ExitByStopLoss = "ExitByStopLoss"

class RMRejectReason(str, Enum):
    NONE = ""
    StrategyNotFound = "StrategyNotFound"
    InsufficientUnit = "InsufficientUnit"
    StrategyAmountExceeded = "StrategyAmountExceeded"
    DailyTransactionAmountExceeded = "DailyTransactionAmountExceeded"

class Event(Enum):
    OrderCallback = 1
    TradeCallback = 2
    PositionsCallback = 3
    Signal = 4
    Quote = 5

class Strategy(BaseModel):
    id: int
    name: str
    add_date: Optional[dt.date]
    status: bool
    leverage_ratio: Optional[float]
    expected_mdd: Optional[float]
    expected_daily_return: Optional[float]
    holding_period: Optional[int]
    order_low_ratio: Optional[float]
    exit_stop_loss: Optional[float]
    exit_take_profit: Optional[float]
    exit_dp_days: Optional[int]
    exit_dp_profit_limit: Optional[float]


class Signal(BaseModel):
    id: str
    source: SignalSource
    sdate: dt.date
    stime: dt.time
    strategy_id: int
    security_type: SecurityType
    code: str
    order_type: OrderType
    price_type: Optional[PriceType]
    action: Action
    quantity: int
    price: Decimal
    exit_type: Optional[ExitType]
    rm_validated: bool = False
    rm_reject_reason: RMRejectReason = RMRejectReason.NONE


class SF31Order(BaseModel):
    signal_id: str
    sfdate: dt.date
    sftime: dt.time
    strategy_id: int
    security_type: SecurityType
    code: str
    order_type: OrderType
    price_type: Optional[PriceType]
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
    price_type: Optional[PriceType]
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
    strategy: int
    code: str
    action: Action
    qty: int
    cost_amt: float
    avg_prc: float
    first_entry_date: dt.date


class QuoteSnapshot(BaseModel):
    dt: dt.datetime
    code: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    total_volume: int
    amount: int
    total_amount: int
    buy_price: float
    buy_volume: int
    sell_price: float
    sell_volume: int


class SF31Position(BaseModel):
    trader_id: str
    ptime: dt.time
    security_type: SecurityType
    code: str
    action: Action
    shares: int
    avg_price: Decimal
    closed_pnl: Decimal
    open_pnl: Decimal
    pnl_chg: Decimal
    cum_return: float


class Contract(BaseModel):
    code: str
    name: str
    reference: Decimal
    limit_up: Decimal
    limit_down: Decimal
    update_date: Optional[dt.date]
    # day_trade: Optional[bool]
