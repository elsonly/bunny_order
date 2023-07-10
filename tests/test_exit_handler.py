import pytest
from pytest_mock import MockerFixture
from typing import List, Dict
import datetime
from decimal import Decimal

from bunny_order.exit_handler import ExitHandler
from bunny_order.models import (
    Position,
    Strategy,
    Contract,
    QuoteSnapshot,
    Event,
    Signal,
    Action,
    ExitType,
)
from bunny_order.common import Positions, Strategies, Contracts, TradingDates


@pytest.fixture(name="exit_handler")
def exit_handler(
    strategies: Strategies,
    positions: Positions,
    contracts: Contracts,
    trading_dates: TradingDates,
):
    return ExitHandler(
        strategies=strategies,
        positions=positions,
        contracts=contracts,
        trading_dates=trading_dates,
    )


def test_send_exit_signal(exit_handler: ExitHandler):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Buy,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )

    exit_handler.send_exit_signal(position, ExitType.ExitByOutDate)
    event, signal = exit_handler.q_out.popleft()

    assert event == Event.Signal
    assert isinstance(signal, Signal)
    assert signal.strategy_id == position.strategy
    assert signal.code == position.code
    assert signal.action != position.action
    assert signal.quantity == position.qty
    assert signal.exit_type == ExitType.ExitByOutDate


def test_exit_by_out_date(freezer, mocker: MockerFixture, exit_handler: ExitHandler):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Buy,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )
    strategy = Strategy(
        id=1,
        name="法說會前主力蠢蠢欲動",
        add_date=datetime.date(2023, 5, 5),
        status=True,
        leverage_ratio=0.64,
        expected_mdd=54.0,
        expected_daily_return=18.74,
        holding_period=10,
        order_low_ratio=-0.8,
        exit_stop_loss=None,
        exit_take_profit=None,
        exit_dp_days=None,
        exit_dp_profit_limit=None,
        enable_dividend=False,
        enable_raise=False,
    )
    freezer.move_to("2023-05-28")
    m_send_exit_signal = mocker.patch.object(exit_handler, "send_exit_signal")
    _ = mocker.patch.object(exit_handler.trading_dates, "_check_updated")
    # no signal
    exit_handler.exit_by_out_date(strategy=strategy, position=position)
    m_send_exit_signal.assert_not_called()

    # no signal
    strategy.holding_period = 2
    exit_handler.exit_by_out_date(strategy=strategy, position=position)
    m_send_exit_signal.assert_not_called()

    # signal
    strategy.holding_period = 1
    exit_handler.exit_by_out_date(strategy=strategy, position=position)
    m_send_exit_signal.assert_called_once_with(position, ExitType.ExitByOutDate)


def test_exit_by_days_profit_limit(
    freezer, mocker: MockerFixture, exit_handler: ExitHandler
):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Buy,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )
    strategy = Strategy(
        id=1,
        name="法說會前主力蠢蠢欲動",
        add_date=datetime.date(2023, 5, 5),
        status=True,
        leverage_ratio=0.64,
        expected_mdd=54.0,
        expected_daily_return=18.74,
        holding_period=10,
        order_low_ratio=-0.8,
        exit_stop_loss=None,
        exit_take_profit=None,
        exit_dp_days=None,
        exit_dp_profit_limit=None,
        enable_dividend=False,
        enable_raise=False,
    )
    snapshot = QuoteSnapshot(
        dt=datetime.datetime(2023, 5, 26, 14, 30),
        code="2836",
        open=12.3,
        high=12.4,
        low=12.3,
        close=12.35,
        volume=4,
        total_volume=269,
        amount=49400,
        total_amount=3325968,
        buy_price=12.35,
        buy_volume=20,
        sell_price=12.4,
        sell_volume=34,
    )

    freezer.move_to("2023-05-30T")
    m_send_exit_signal = mocker.patch.object(exit_handler, "send_exit_signal")
    _ = mocker.patch.object(exit_handler.trading_dates, "_check_updated")
    # no signal
    exit_handler.exit_by_days_profit_limit(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # no signal
    strategy.exit_dp_days = 2  # met
    strategy.exit_dp_profit_limit = -0.1  # not met
    exit_handler.exit_by_days_profit_limit(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # no signal
    strategy.exit_dp_days = 5  # not met
    strategy.exit_dp_profit_limit = 0.1  # met
    exit_handler.exit_by_days_profit_limit(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # signal
    strategy.exit_dp_days = 2  # met
    strategy.exit_dp_profit_limit = 0.1  # met
    exit_handler.exit_by_days_profit_limit(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_called_once_with(position, ExitType.ExitByDaysProfitLimit)


def test_exit_by_take_profit(mocker: MockerFixture, exit_handler: ExitHandler):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Buy,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )
    strategy = Strategy(
        id=1,
        name="法說會前主力蠢蠢欲動",
        add_date=datetime.date(2023, 5, 5),
        status=True,
        leverage_ratio=0.64,
        expected_mdd=54.0,
        expected_daily_return=18.74,
        holding_period=10,
        order_low_ratio=-0.8,
        exit_stop_loss=None,
        exit_take_profit=None,
        exit_dp_days=None,
        exit_dp_profit_limit=None,
        enable_dividend=False,
        enable_raise=False,
    )
    snapshot = QuoteSnapshot(
        dt=datetime.datetime(2023, 5, 26, 14, 30),
        code="2836",
        open=12.3,
        high=12.4,
        low=12.3,
        close=12.35,
        volume=4,
        total_volume=269,
        amount=49400,
        total_amount=3325968,
        buy_price=12.35,
        buy_volume=20,
        sell_price=12.4,
        sell_volume=34,
    )

    m_send_exit_signal = mocker.patch.object(exit_handler, "send_exit_signal")
    # no signal
    strategy.exit_take_profit = 0.1  # not met
    exit_handler.exit_by_take_profit(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # signal
    strategy.exit_take_profit = -0.1  # met
    exit_handler.exit_by_take_profit(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_called_once_with(position, ExitType.ExitByTakeProfit)


def test_exit_by_stop_loss(mocker: MockerFixture, exit_handler: ExitHandler):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Buy,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )
    strategy = Strategy(
        id=1,
        name="法說會前主力蠢蠢欲動",
        add_date=datetime.date(2023, 5, 5),
        status=True,
        leverage_ratio=0.64,
        expected_mdd=54.0,
        expected_daily_return=18.74,
        holding_period=10,
        order_low_ratio=-0.8,
        exit_stop_loss=None,
        exit_take_profit=None,
        exit_dp_days=None,
        exit_dp_profit_limit=None,
        enable_dividend=False,
        enable_raise=False,
    )
    snapshot = QuoteSnapshot(
        dt=datetime.datetime(2023, 5, 26, 14, 30),
        code="2836",
        open=12.3,
        high=12.4,
        low=12.3,
        close=12.35,
        volume=4,
        total_volume=269,
        amount=49400,
        total_amount=3325968,
        buy_price=12.35,
        buy_volume=20,
        sell_price=12.4,
        sell_volume=34,
    )

    m_send_exit_signal = mocker.patch.object(exit_handler, "send_exit_signal")
    # no signal
    strategy.exit_stop_loss = -0.1  # not met
    exit_handler.exit_by_stop_loss(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # signal
    strategy.exit_stop_loss = 0.1  # met
    exit_handler.exit_by_stop_loss(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_called_once_with(position, ExitType.ExitByStopLoss)


def test_exit_by_profit_pullback_long(mocker: MockerFixture, exit_handler: ExitHandler):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Buy,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )
    strategy = Strategy(
        id=1,
        name="法說會前主力蠢蠢欲動",
        add_date=datetime.date(2023, 5, 5),
        status=True,
        leverage_ratio=0.64,
        expected_mdd=54.0,
        expected_daily_return=18.74,
        holding_period=10,
        order_low_ratio=-0.8,
        exit_stop_loss=None,
        exit_take_profit=None,
        exit_dp_days=None,
        exit_dp_profit_limit=None,
        enable_dividend=False,
        enable_raise=False,
        exit_profit_pullback_ratio=0.5,
        exit_profit_pullback_threshold=0.03,
    )
    snapshot = QuoteSnapshot(
        dt=datetime.datetime(2023, 5, 26, 14, 30),
        code="2836",
        open=12.3,
        high=12.4,
        low=12.3,
        close=12.35,
        volume=4,
        total_volume=269,
        amount=49400,
        total_amount=3325968,
        buy_price=12.35,
        buy_volume=20,
        sell_price=12.4,
        sell_volume=34,
    )

    m_send_exit_signal = mocker.patch.object(exit_handler, "send_exit_signal")
    # no signal
    strategy.exit_profit_pullback_threshold = 0.1  # not met
    strategy.exit_profit_pullback_ratio = 0.5  # met
    exit_handler.exit_by_profit_pullback(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # signal
    strategy.exit_profit_pullback_threshold = 0.03  # met
    strategy.exit_profit_pullback_ratio = 0.5  # met
    exit_handler.exit_by_profit_pullback(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_called_once_with(position, ExitType.ExitByProfitPullback)


def test_exit_by_profit_pullback_short(mocker: MockerFixture, exit_handler: ExitHandler):
    position = Position(
        strategy=1,
        code="2836",
        action=Action.Sell,
        qty=3,
        cost_amt=37200.0,
        avg_prc=12.4,
        first_entry_date=datetime.date(2023, 5, 25),
        low_since_entry=11.4,
        high_since_entry=13.4,
    )
    strategy = Strategy(
        id=1,
        name="法說會前主力蠢蠢欲動",
        add_date=datetime.date(2023, 5, 5),
        status=True,
        leverage_ratio=0.64,
        expected_mdd=54.0,
        expected_daily_return=18.74,
        holding_period=10,
        order_low_ratio=-0.8,
        exit_stop_loss=None,
        exit_take_profit=None,
        exit_dp_days=None,
        exit_dp_profit_limit=None,
        enable_dividend=False,
        enable_raise=False,
        exit_profit_pullback_ratio=0.5,
        exit_profit_pullback_threshold=0.03,
    )
    snapshot = QuoteSnapshot(
        dt=datetime.datetime(2023, 5, 26, 14, 30),
        code="2836",
        open=12.3,
        high=12.4,
        low=12.3,
        close=12.35,
        volume=4,
        total_volume=269,
        amount=49400,
        total_amount=3325968,
        buy_price=12.35,
        buy_volume=20,
        sell_price=12.4,
        sell_volume=34,
    )

    m_send_exit_signal = mocker.patch.object(exit_handler, "send_exit_signal")
    # no signal
    strategy.exit_profit_pullback_threshold = 0.1  # not met
    strategy.exit_profit_pullback_ratio = 0.5  # met
    exit_handler.exit_by_profit_pullback(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_not_called()

    # signal
    strategy.exit_profit_pullback_threshold = 0.03  # met
    strategy.exit_profit_pullback_ratio = 0.5  # met
    exit_handler.exit_by_profit_pullback(
        strategy=strategy, position=position, snapshot=snapshot
    )
    m_send_exit_signal.assert_called_once_with(position, ExitType.ExitByProfitPullback)
