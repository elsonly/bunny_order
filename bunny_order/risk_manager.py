from typing import Union, List, Deque, Dict
from decimal import Decimal
import pandas as pd

from bunny_order.models import (
    Contract,
    Signal,
    Strategy,
    RMRejectReason,
    SignalSource,
    Action,
)
from bunny_order.common import (
    Strategies,
    Contracts,
    Positions,
    ComingDividends,
    TradingDates,
)
from bunny_order.utils import logger, get_tpe_datetime
from bunny_order.config import Config


class RiskManager:
    def __init__(
        self,
        strategies: Strategies,
        contracts: Contracts,
        positions: Positions,
        coming_dividends: ComingDividends,
        trading_dates: TradingDates,
    ):
        self.strategies = strategies
        self.contracts = contracts
        self.positions = positions
        self.coming_dividends = coming_dividends
        self.trading_dates = trading_dates
        self.daily_amount_limit = Config.OM_DAILY_AMOUNT_LIMIT
        self.cumulative_amount = 0

    def validate_signal(self, signal: Signal):
        if not self._validate_strategy(signal):
            return
        if signal.source == SignalSource.XQ:
            self.qty_leverage_ratio_adjustment(signal)
            self.price_limit_adjustment(signal)

        if not self._validate_trade_datetime(signal):
            return
        if not self._validate_latest_contract(signal):
            return
        if not self._validate_dividend_date(signal):
            return
        if not self._validate_quantity_unit(signal):
            return
        if not self._validate_daily_transaction_amount_limit(signal):
            return
        if not self._validate_strategy_amount_limit(signal):
            return
        signal.rm_validated = True

    def _validate_latest_contract(self, signal: Signal) -> bool:
        if not self.contracts.exists(signal.code):
            logger.warning(f"contract not found: {signal.code}")
            return False

        if not self.contracts.check_updated([signal.code]):
            logger.warning(f"contract outdated: {signal.code}")
            return False

        return True

    def _validate_raise(self, signal: Signal) -> bool:
        if signal.action == Action.Buy:
            if not self.strategies.get_strategy(signal.strategy_id).enable_raise:
                if self.positions.exists(signal.strategy_id, signal.code):
                    signal.rm_reject_reason = RMRejectReason.DisableRaise
                    logger.warning(f"reject signal: {signal}")
                    return False
        return True

    def _validate_dividend_date(self, signal: Signal) -> bool:
        if signal.action == Action.Buy:
            strategy = self.strategies.get_strategy(signal.strategy_id)
            if (
                strategy.holding_period
                and self.coming_dividends.exists(signal.code)
                and not strategy.enable_dividend
            ):
                coming_dividend = self.coming_dividends.get_coming_dividend(signal.code)
                if (
                    self.trading_dates.get_next_n_trading_date(
                        days=strategy.holding_period
                    )
                    >= coming_dividend.ex_date
                ):
                    signal.rm_reject_reason = RMRejectReason.CannotParticipatingDividend
                    logger.warning(f"reject signal: {signal}")
                    return False
        return True

    def _validate_strategy(self, signal: Signal) -> bool:
        if not self.strategies.exists(signal.strategy_id):
            signal.rm_reject_reason = RMRejectReason.StrategyNotFound
            logger.warning(f"reject signal: {signal}")
            return False
        if not self.strategies.get_strategy(signal.strategy_id).status:
            signal.rm_reject_reason = RMRejectReason.StrategyInactive
            logger.warning(f"reject signal: {signal}")
            return False
        return True

    def qty_leverage_ratio_adjustment(self, signal: Signal):
        if signal.action == Action.Buy:
            signal.quantity = int(
                signal.quantity
                * self.strategies.get_strategy(signal.strategy_id).leverage_ratio
            )

    def price_limit_adjustment(self, signal: Signal):
        contract = self.contracts.get_contract(signal.code)
        if signal.action == Action.Buy:
            signal.price = contract.limit_up
        else:
            signal.price = contract.limit_down

    def _validate_trade_datetime(self, signal: Signal) -> bool:
        if Config.DEBUG:
            return True
        if signal.sdate.weekday() >= 5:
            signal.rm_reject_reason = RMRejectReason.InvalidTradeHour
            return False
        return True

    def _validate_quantity_unit(self, signal: Signal) -> bool:
        if signal.quantity < 1:
            signal.rm_reject_reason = RMRejectReason.InsufficientUnit
            logger.warning(f"reject signal: {signal}")
            return False
        return True

    def _validate_daily_transaction_amount_limit(self, signal: Signal) -> bool:
        # TODO
        return True

    def _validate_strategy_amount_limit(self, signal: Signal) -> bool:
        # TODO
        return True
