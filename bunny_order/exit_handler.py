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
)
from bunny_order.config import Config


class ExitHandler:
    def __init__(
        self,
        strategies: Dict[int, Strategy],
        positions: Dict[str, Dict[str, Position]],
        contracts: Dict[str, Contract],
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

    def reset(self):
        self.running_signals.clear()

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
        if signal.action == Action.Sell:
            signal.price = self.contracts[position.code].limit_down
        else:
            signal.price = self.contracts[position.code].limit_up

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
        if get_tpe_datetime().hour < 9 or get_tpe_datetime().hour >= 14:
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
        if get_tpe_datetime().hour < 9 or get_tpe_datetime().hour >= 14:
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

    def on_quote(self, snapshots: Dict[str, QuoteSnapshot]):
        for strategy_id, d0 in self.positions.items():
            for code, position in d0.items():
                if self.is_running_signal(strategy_id, code):
                    continue

                self.exit_by_out_date(self.strategies[strategy_id], position)
                self.exit_by_days_profit_limit(
                    self.strategies[strategy_id], position, snapshots[code]
                )
                self.exit_by_take_profit(
                    self.strategies[strategy_id], position, snapshots[code]
                )
                self.exit_by_stop_loss(
                    self.strategies[strategy_id], position, snapshots[code]
                )

    def run(self):
        logger.info("Start Exit Handler")
        while not self.active_event.isSet():
            try:
                if self.q_in:
                    event, data = self.q_in.pop()
                    if event == Event.Quote:
                        self.on_quote(data)
                    else:
                        logger.warning(f"Invalid event: {event}")

            except Exception as e:
                logger.exception(e)
            time.sleep(0.01)
        logger.info("Shutdown Exit Handler")
