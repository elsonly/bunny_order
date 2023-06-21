import pytest
from pytest_mock import MockerFixture
from typing import List, Dict
import datetime
from decimal import Decimal
import datetime as dt

from bunny_order.order_manager import OrderManager, SignalCollector
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
from bunny_order.common import Positions, Strategies, Contracts, Snapshots, TradingDates


@pytest.fixture()
def order_manager(
    strategies: Strategies, contracts: Contracts, trading_dates: TradingDates
):
    return OrderManager(
        strategies=strategies, contracts=contracts, trading_dates=trading_dates
    )


@pytest.fixture()
def signal_collector(mocker: MockerFixture, contracts: Dict[str, Contract]):
    return SignalCollector(dm=mocker, contracts=contracts)


def create_signal(
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
):
    return Signal(
        id=id,
        source=source,
        sdate=sdate,
        stime=stime,
        strategy_id=strategy_id,
        security_type=security_type,
        code=code,
        order_type=order_type,
        price_type=price_type,
        action=action,
        quantity=quantity,
        price=price,
        exit_type=exit_type,
        rm_validated=rm_validated,
        rm_reject_reason=rm_reject_reason,
    )


@pytest.fixture()
def signal() -> Signal:
    return create_signal()


def test_on_signal(signal_collector: SignalCollector, signal: Signal):
    signal_collector.on_signal(signal)
    assert len(signal_collector.collector) > 0


@pytest.mark.parametrize(
    "in_signals, offsetting_signals, out_signals",
    [
        # q_buy == q_sell
        (
            {
                Action.Buy: [create_signal(quantity=4, action=Action.Buy)],
                Action.Sell: [create_signal(quantity=4, action=Action.Sell)],
            },
            [
                create_signal(quantity=4, action=Action.Buy),
                create_signal(quantity=4, action=Action.Sell),
            ],
            [
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy > q_sell
        (
            {
                Action.Buy: [create_signal(quantity=4, action=Action.Buy)],
                Action.Sell: [create_signal(quantity=2, action=Action.Sell)],
            },
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
            ],
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy < q_sell
        (
            {
                Action.Buy: [create_signal(quantity=2, action=Action.Buy)],
                Action.Sell: [create_signal(quantity=4, action=Action.Sell)],
            },
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
            ],
            [
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
            ],
        ),
        # q_buy == q_sell, multiple signals, quantity match
        (
            {
                Action.Buy: [
                    create_signal(quantity=2, action=Action.Buy),
                    create_signal(quantity=2, action=Action.Buy),
                ],
                Action.Sell: [
                    create_signal(quantity=2, action=Action.Sell),
                    create_signal(quantity=2, action=Action.Sell),
                ],
            },
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=2, action=Action.Sell),
            ],
            [
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy > q_sell, multiple signals
        (
            {
                Action.Buy: [
                    create_signal(quantity=4, action=Action.Buy),
                    create_signal(quantity=2, action=Action.Buy),
                ],
                Action.Sell: [
                    create_signal(quantity=2, action=Action.Sell),
                    create_signal(quantity=2, action=Action.Sell),
                ],
            },
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=2, action=Action.Sell),
            ],
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy < q_sell, multiple signals
        (
            {
                Action.Buy: [
                    create_signal(quantity=2, action=Action.Buy),
                    create_signal(quantity=2, action=Action.Buy),
                ],
                Action.Sell: [
                    create_signal(quantity=2, action=Action.Sell),
                    create_signal(quantity=4, action=Action.Sell),
                ],
            },
            [
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=2, action=Action.Sell),
            ],
            [
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy == q_sell, multiple signals, signals not match
        (
            {
                Action.Buy: [
                    create_signal(quantity=2, action=Action.Buy),
                    create_signal(quantity=4, action=Action.Buy),
                ],
                Action.Sell: [
                    create_signal(quantity=1, action=Action.Sell),
                    create_signal(quantity=3, action=Action.Sell),
                    create_signal(quantity=1, action=Action.Sell),
                    create_signal(quantity=1, action=Action.Sell),
                ],
            },
            [
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
            ],
            [
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy > q_sell, multiple signals, signals not match
        (
            {
                Action.Buy: [
                    create_signal(quantity=2, action=Action.Buy),
                    create_signal(quantity=4, action=Action.Buy),
                    create_signal(quantity=6, action=Action.Buy),
                ],
                Action.Sell: [
                    create_signal(quantity=1, action=Action.Sell),
                    create_signal(quantity=3, action=Action.Sell),
                ],
            },
            [
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=2, action=Action.Buy),
            ],
            [
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=2, action=Action.Buy),
                create_signal(quantity=6, action=Action.Buy),
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=0, action=Action.Sell),
            ],
        ),
        # q_buy < q_sell, multiple signals, signals not match
        (
            {
                Action.Buy: [
                    create_signal(quantity=1, action=Action.Buy),
                    create_signal(quantity=3, action=Action.Buy),
                ],
                Action.Sell: [
                    create_signal(quantity=2, action=Action.Sell),
                    create_signal(quantity=4, action=Action.Sell),
                    create_signal(quantity=6, action=Action.Sell),
                ],
            },
            [
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=1, action=Action.Sell),
                create_signal(quantity=1, action=Action.Buy),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=2, action=Action.Buy),
            ],
            [
                create_signal(quantity=0, action=Action.Sell),
                create_signal(quantity=2, action=Action.Sell),
                create_signal(quantity=6, action=Action.Sell),
                create_signal(quantity=0, action=Action.Buy),
                create_signal(quantity=0, action=Action.Buy),
            ],
        ),
    ],
)
def test__offset_signals(
    signal_collector: SignalCollector,
    in_signals: Dict[Action, Signal],
    offsetting_signals: List[Signal],
    out_signals: List[Signal],
):
    signal_collector._offset_signals(in_signals)
    assert all(
        [x in offsetting_signals for x in signal_collector._offsetting_signals]
    ) and len(offsetting_signals) == len(signal_collector._offsetting_signals)
    assert all([x in out_signals for x in signal_collector._signals]) and len(
        out_signals
    ) == len(signal_collector._signals)


def test_check_signals(signal_collector: SignalCollector, mocker: MockerFixture):
    test_collector = {
        create_signal().code: {
            Action.Buy: [create_signal(action=Action.Buy, quantity=4)],
            Action.Sell: [create_signal(action=Action.Sell, quantity=4)],
        }
    }
    signal_collector.collector = test_collector
    signal_collector.check_signals()
    assert len(signal_collector._signals) == 2
    assert len(signal_collector._offsetting_signals) == 2


def test_execute_offsetting_signals(
    signal_collector: SignalCollector, mocker: MockerFixture
):
    m__place_mock_order = mocker.patch.object(signal_collector, "_place_mock_order")
    offsetting_signals = [
        create_signal(quantity=1, action=Action.Sell),
        create_signal(quantity=1, action=Action.Buy),
        create_signal(quantity=1, action=Action.Sell),
        create_signal(quantity=1, action=Action.Buy),
        create_signal(quantity=2, action=Action.Sell),
        create_signal(quantity=2, action=Action.Buy),
    ]
    expected_call_count = len(offsetting_signals)
    signal_collector._offsetting_signals = offsetting_signals
    signal_collector.execute_offsetting_signals()

    assert m__place_mock_order.call_count == expected_call_count


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
    strategies: Strategies,
    signal: Signal,
):
    strategies.get_strategy(signal.strategy_id).order_low_ratio = -2.35
    expected = Decimal("43.00")

    assert order_manager.price_order_low_ratio_adjustment(signal) == expected


def test_execute_orders_half_open_half_order_low_ratio(
    mocker: MockerFixture,
    order_manager: OrderManager,
    signal: Signal,
):
    m_place_order = mocker.patch.object(order_manager, "place_order")
    order_manager.execute_orders_half_open_half_order_low_ratio(signal)

    assert m_place_order.call_count == 2


def test_execute_limit_order(
    freezer,
    mocker: MockerFixture,
    order_manager: OrderManager,
    strategies: Strategies,
    signal: Signal,
):
    m_place_order = mocker.patch.object(order_manager, "place_order")
    # m_debug = mocker.patch("bunny_order.utils.Config")
    # m_debug.DEBUG = False
    # m_debug.TRADE_START_TIME = dt.time(hour=8, minute=40)
    # m_debug.TRADE_END_TIME = dt.time(hour=14, minute=30)

    # signal
    order_manager.execute_limit_order(signal)
    m_place_order.assert_called_once()
