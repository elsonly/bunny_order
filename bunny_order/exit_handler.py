from typing import Dict, Deque, DefaultDict, Tuple, List
import datetime as dt
import pandas as pd
from collections import defaultdict, deque
import time
import threading

from bunny_order.models import (
    QuoteSnapshot,
    Strategy,
    Position,
    Signal,
    OrderType,
    Action,
    Contract,
    ExitType,
    SecurityType,
    PriceType,
    SignalSource,
    Event,
)
from bunny_order.utils import (
    get_tpe_datetime,
    get_signal_id,
    logger,
    dump_checkpoints,
    load_checkpoints,
    is_trade_date,
    is_trade_time,
    is_signal_time,
)
from bunny_order.common import Strategies, Snapshots, Positions, Contracts
from bunny_order.config import Config


class ExitHandler:
    def __init__(
        self,
        strategies: Strategies,
        positions: Positions,
        contracts: Contracts,
        q_in: Deque[Tuple[Event, Dict[str, QuoteSnapshot]]] = deque(),
        q_out: Deque[Tuple[Event, Signal]] = deque(),
        active_event: threading.Event = threading.Event(),
    ):
        self.q_in = q_in
        self.q_out = q_out
        self.active_event = active_event
        self.strategies = strategies
        self.positions = positions
        self.contracts = contracts
        self.running_signals: DefaultDict[int, List[str]] = defaultdict(list)
        self.checkpoints_path = f"{Config.CHECKPOINTS_DIR}/exit_handler.json"
        self.running_signals.update(load_checkpoints(self.checkpoints_path))
        self.quote_delay_tolerance = Config.QUOTE_DELAY_TOLERANCE

    def reset(self):
        self.running_signals.clear()
        dump_checkpoints(self.checkpoints_path, self.running_signals)

    def send_exit_signal(self, position: Position, exit_type: ExitType):
        signal = Signal(
            id=get_signal_id(),
            source=SignalSource.ExitHandler,
            sdate=get_tpe_datetime().date(),
            stime=get_tpe_datetime().time(),
            strategy_id=position.strategy,
            security_type=SecurityType.Stock,
            code=position.code,
            order_type=OrderType.ROD,
            price_tpye=PriceType.LMT,
            action=Action.Sell if position.action == Action.Buy else Action.Buy,
            quantity=position.qty,
            price=0,
            exit_type=exit_type,
        )
        contract = self.contracts.get_contract(position.code)
        if signal.action == Action.Sell:
            signal.price = contract.limit_down
        else:
            signal.price = contract.limit_up

        self.q_out.append((Event.Signal, signal))
        self.running_signals[signal.strategy_id].append(signal.code)
        dump_checkpoints(self.checkpoints_path, self.running_signals)

    def exit_by_out_date(self, strategy: Strategy, position: Position):
        if self.is_running_signal(strategy.id, position.code):
            return
        if strategy.holding_period is None:
            return
        if position.first_entry_date is None:
            return
        # TODO: should consider holiday in business day
        if (
            get_tpe_datetime().date()
            >= (
                position.first_entry_date + pd.offsets.BDay(strategy.holding_period)
            ).date()
        ):
            self.send_exit_signal(position, ExitType.ExitByOutDate)

    def exit_by_days_profit_limit(
        self, strategy: Strategy, position: Position, snapshot: QuoteSnapshot
    ):
        if self.is_running_signal(strategy.id, position.code):
            return
        if strategy.exit_dp_days is None or strategy.exit_dp_profit_limit is None:
            return
        # TODO: should consider holiday in business day
        if (
            get_tpe_datetime().date()
            >= (
                position.first_entry_date + strategy.exit_dp_days * pd.offsets.BDay()
            ).date()
        ):
            if position.action == Action.Buy:
                if (
                    snapshot.close / position.avg_prc - 1
                    <= strategy.exit_dp_profit_limit
                ):
                    self.send_exit_signal(position, ExitType.ExitByDaysProfitLimit)
            else:
                if (
                    position.avg_prc / snapshot.close - 1
                    <= strategy.exit_dp_profit_limit
                ):
                    self.send_exit_signal(position, ExitType.ExitByDaysProfitLimit)

    def exit_by_take_profit(
        self, strategy: Strategy, position: Position, snapshot: QuoteSnapshot
    ):
        if self.is_running_signal(strategy.id, position.code):
            return
        if strategy.exit_take_profit is None:
            return

        if position.action == Action.Buy:
            if snapshot.close / position.avg_prc - 1 >= strategy.exit_take_profit:
                self.send_exit_signal(position, ExitType.ExitByTakeProfit)
        else:
            if position.avg_prc / snapshot.close - 1 >= strategy.exit_take_profit:
                self.send_exit_signal(position, ExitType.ExitByTakeProfit)

    def exit_by_stop_loss(
        self, strategy: Strategy, position: Position, snapshot: QuoteSnapshot
    ):
        if self.is_running_signal(strategy.id, position.code):
            return
        if strategy.exit_stop_loss is None:
            return

        if position.action == Action.Buy:
            if snapshot.close / position.avg_prc - 1 <= strategy.exit_stop_loss:
                self.send_exit_signal(position, ExitType.ExitByStopLoss)
        else:
            if position.avg_prc / snapshot.close - 1 <= strategy.exit_stop_loss:
                self.send_exit_signal(position, ExitType.ExitByStopLoss)

    def is_running_signal(self, strategy_id: int, code: str) -> bool:
        return (
            strategy_id in self.running_signals
            and code in self.running_signals[strategy_id]
        )

    def on_quote(self, snapshots: Snapshots):
        strategy_codes = self.positions.get_position_strategy_codes()
        for strategy_id, code in strategy_codes:
            if self.is_running_signal(strategy_id, code):
                continue
            snapshot = snapshots.get_snapshot(code)
            if (get_tpe_datetime() - snapshot.dt).seconds > self.quote_delay_tolerance:
                continue
            # skip matching order
            if snapshot.total_volume == 0:
                continue
            if snapshot.volume == 0:
                continue
            strategy = self.strategies.get_strategy(strategy_id)
            position = self.positions.get_position(strategy_id, code)
            self.exit_by_days_profit_limit(strategy, position, snapshot)
            self.exit_by_take_profit(strategy, position, snapshot)
            self.exit_by_stop_loss(strategy, position, snapshot)

    def before_market_signals(self):
        strategy_codes = self.positions.get_position_strategy_codes()
        for strategy_id, code in strategy_codes:
            if self.is_running_signal(strategy_id, code):
                continue
            strategy = self.strategies.get_strategy(strategy_id)
            position = self.positions.get_position(strategy_id, code)
            self.exit_by_out_date(strategy, position)

    def system_check(self) -> bool:
        if not is_trade_date():
            return False
        if not self.contracts.check_updated():
            if is_trade_time():
                logger.warning(
                    f"contracts not updated, previous update time: {self.contracts.update_dt}"
                )
            return False
        if not self.positions.check_updated():
            if is_trade_time():
                logger.warning(
                    f"positions not updated, previous update time: {self.positions.update_dt}"
                )
            return False
        if not self.strategies.check_updated():
            if is_trade_time():
                logger.warning(
                    f"strategies not updated, previous update time: {self.strategies.update_dt}"
                )
            return False
        return True

    def run(self):
        logger.info("Start Exit Handler")
        while not self.active_event.isSet():
            try:
                if not self.system_check():
                    time.sleep(10)
                    continue

                if self.q_in:
                    event, data = self.q_in.popleft()
                    if event == Event.Quote:
                        if is_trade_time():
                            self.on_quote(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

                if is_signal_time():
                    self.before_market_signals()

            except Exception as e:
                logger.exception(e)

            time.sleep(0.01)
        logger.info("Shutdown Exit Handler")
