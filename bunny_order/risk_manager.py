from typing import Union, List, Deque, Dict
from decimal import Decimal

from bunny_order.models import (
    Contract,
    Signal,
    Strategy,
    RMRejectReason,
    SignalSource,
)
from bunny_order.common import Strategies, Contracts, Positions
from bunny_order.utils import logger
from bunny_order.config import Config


class RiskManager:
    def __init__(
        self,
        strategies: Strategies,
        contracts: Contracts,
        positions: Positions,
    ):
        self.strategies = strategies
        self.contracts = contracts
        self.positions = positions
        self.daily_amount_limit = Config.OM_DAILY_AMOUNT_LIMIT
        self.cumulative_amount = 0

    def validate_signal(self, signal: Signal):
        if not self._validate_strategy(signal):
            return
        if signal.source == SignalSource.XQ:
            self.qty_leverage_ratio_adjustment(signal)
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
        if not self.strategies.get_strategy(signal.strategy_id).enable_raise:
            if self.positions.exists(signal.strategy_id, signal.code):
                signal.rm_reject_reason = RMRejectReason.DisableRaise
                logger.warning(f"reject signal: {signal}")
                return False
        return True

    def _validate_dividend_date(self, signal: Signal) -> bool:
        # TODO
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

    def qty_leverage_ratio_adjustment(self, signal: Signal) -> int:
        signal.quantity = int(
            signal.quantity
            * self.strategies.get_strategy(signal.strategy_id).leverage_ratio
        )

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
