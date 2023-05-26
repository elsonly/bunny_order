import pytest
from pytest_mock import MockerFixture
from typing import List, Dict
import datetime
from decimal import Decimal

from bunny_order.risk_manager import RiskManager
from bunny_order.models import (
    Strategy,
    Contract,
    Signal,
    Action,
    ExitType,
    SecurityType,
    OrderType,
    SignalSource,
    RMRejectReason,
)


@pytest.fixture()
def risk_manager(strategies: Dict[int, Strategy], contracts: Dict[str, Contract]):
    return RiskManager(contracts=contracts, strategies=strategies)


@pytest.fixture()
def signal() -> Signal:
    return Signal(
        id="002",
        source=SignalSource.XQ,
        sdate=datetime.date(2023, 5, 28),
        stime=datetime.time(23, 30, 18, 434279),
        strategy_id=1,
        security_type=SecurityType.Stock,
        code="2882",
        order_type=OrderType.ROD,
        price_type=None,
        action=Action.Sell,
        quantity=12,
        price=Decimal("39.65"),
        exit_type=ExitType.ExitByOutDate,
        rm_validated=True,
        rm_reject_reason=RMRejectReason.NONE,
    )