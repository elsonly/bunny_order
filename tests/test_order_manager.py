import pytest
from pytest_mock import MockerFixture
from typing import List, Dict
import datetime
from decimal import Decimal
import datetime as dt

from bunny_order.order_manager import OrderManager
from bunny_order.models import (
    Strategy,
    Contract,
    Signal,
    Action,
    ExitType,
    SF31Order,
    SecurityType,
    OrderType,
    SignalSource,
    RMRejectReason,
)
from bunny_order.config import Config


@pytest.fixture(name="order_manager")
def order_manager(strategies: Dict[int, Strategy], contracts: Dict[str, Contract]):
    return OrderManager(strategies=strategies, contracts=contracts)


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


def test_place_order(order_manager: OrderManager, tmp_path):
    order = SF31Order(
        signal_id="004",
        sfdate=datetime.date(2023, 5, 28),
        sftime=datetime.time(23, 30, 18, 698434),
        strategy_id=1,
        security_type=SecurityType.Stock,
        code="4129",
        order_type=OrderType.ROD,
        price_type=None,
        action=Action.Sell,
        quantity=8,
        price=Decimal("56.6"),
        order_id="",
    )
    strategy = "法說會前主力蠢蠢欲動"
    expected = "004,Stock,1685287818.698434,4129,ROD,S,8,56.6\n"

    order_manager.s31_orders_dir = tmp_path
    order_manager.place_order(order)
    path = tmp_path / strategy / "Sell.log"
    assert path.read_text() == expected


def test_cancel_order(order_manager: OrderManager):
    pass


def test_price_order_low_ratio_adjustment(
    order_manager: OrderManager,
    strategies: Dict[int, Strategy],
    signal: Signal,
):
    strategies[signal.strategy_id].order_low_ratio = -2.35
    expected = Decimal("43.00")

    assert order_manager.price_order_low_ratio_adjustment(signal) == expected


def test_excute_orders_half_open_half_order_low_ratio(
    mocker: MockerFixture,
    order_manager: OrderManager,
    signal: Signal,
):
    m_place_order = mocker.patch.object(order_manager, "place_order")
    order_manager.excute_orders_half_open_half_order_low_ratio(signal)

    assert m_place_order.call_count == 2


def test_excute_pre_market_orders(
    freezer,
    mocker: MockerFixture,
    order_manager: OrderManager,
    strategies: Dict[int, Strategy],
    signal: Signal,
):
    m_place_order = mocker.patch.object(order_manager, "place_order")
    m_debug = mocker.patch("bunny_order.utils.Config")
    m_debug.DEBUG = False
    m_debug.TRADE_START_TIME = dt.time(hour=8, minute=40)
    m_debug.TRADE_END_TIME = dt.time(hour=14, minute=30)
    
    # no signal
    freezer.move_to("2023-05-29T00:00:00")
    order_manager.excute_pre_market_orders(signal)
    m_place_order.assert_not_called()

    # signal
    freezer.move_to("2023-05-29T00:40:00")
    order_manager.excute_pre_market_orders(signal)
    m_place_order.assert_called_once()