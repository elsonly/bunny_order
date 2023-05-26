from typing import Union, List, Deque, Dict
from decimal import Decimal

from bunny_order.models import (
    Contract,
    Signal,
    Strategy,
    RMRejectReason,
    SignalSource,
)
from bunny_order.utils import get_tpe_datetime, logger
from bunny_order.config import Config


class RiskManager:
    def __init__(
        self,
        strategies: Dict[int, Strategy],
        contracts: Dict[str, Contract],
    ):
        self.strategies = strategies
        self.contracts = contracts
        self.daily_amount_limit = Config.OM_DAILY_AMOUNT_LIMIT
        self.cumulative_amount = 0

    def validate_signal(self, signal: Signal):
        if not self._validate_strategy(signal):
            return
        if signal.source == SignalSource.XQ:
            self.qty_leverage_ratio_adjustment(signal)
        if not self._validate_quantity_unit(signal):
            return
        if not self._validate_daily_transaction_amount_limit(signal):
            return
        if not self._validate_strategy_amount_limit(signal):
            return
        signal.rm_validated = True

    def _validate_strategy(self, signal: Signal) -> bool:
        if signal.strategy_id not in self.strategies:
            signal.rm_reject_reason = RMRejectReason.StrategyNotFound
            logger.warning(f"reject signal: {signal}")
            return False
        return True

    def qty_leverage_ratio_adjustment(self, signal: Signal) -> int:
        signal.quantity = int(
            signal.quantity * self.strategies[signal.strategy_id].leverage_ratio
        )

    def _validate_quantity_unit(self, signal: Signal) -> bool:
        if signal.quantity < 1:
            signal.rm_reject_reason = RMRejectReason.InsufficientUnit
            logger.warning(f"reject signal: {signal}")
            return False
        return True

    def _validate_daily_transaction_amount_limit(self, signal: Signal) -> bool:
        return True

    def _validate_strategy_amount_limit(self, signal: Signal) -> bool:
        return True
