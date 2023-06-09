from typing import Dict, List, Tuple
import datetime as dt

from bunny_order.models import (
    Strategy,
    Position,
    Contract,
    QuoteSnapshot,
    ComingDividend,
)
from bunny_order.utils import ReadWriteLock, get_tpe_datetime
from bunny_order.config import Config


class Strategies:
    def __init__(self, tolerance: int = 60):
        self._data: Dict[int, Strategy] = {}
        self.lock = ReadWriteLock()
        self.update_dt: dt.datetime = None
        self.tolerance = tolerance

    def update(self, data: Dict[int, Strategy]):
        self.lock.acquire_write()
        self._data.clear()
        self._data.update(data)
        self.lock.release_write()
        self.update_dt = get_tpe_datetime()

    def _check_updated(self):
        if (
            self.update_dt is None
            or (get_tpe_datetime() - self.update_dt).seconds > self.tolerance
        ):
            raise Exception(
                f"strategy outdated, previous update time: {self.update_dt}"
            )

    def check_updated(self) -> bool:
        if (
            self.update_dt is None
            or (get_tpe_datetime() - self.update_dt).seconds > self.tolerance
        ):
            return False
        return True

    def exists(self, id: int) -> bool:
        self._check_updated()
        return id in self._data

    def get_strategy(self, strategy_id: int) -> Strategy:
        self._check_updated()
        if strategy_id in self._data:
            return self._data[strategy_id]
        raise Exception(f"cannot find strategy_id: {strategy_id}")

    def get_id(self, name: str) -> int:
        self._check_updated()
        strategy_id = 7
        try:
            self.lock.acquire_read()
            for _id, strategy in self._data.items():
                if name == strategy.name:
                    strategy_id = strategy.id
        finally:
            self.lock.release_read()

        return strategy_id


class Snapshots:
    def __init__(self, tolerance: int = 60):
        self._data: Dict[str, QuoteSnapshot] = {}
        self.lock = ReadWriteLock()
        self.update_dt: dt.datetime = None
        self.tolerance = tolerance

    def update(self, data: Dict[int, QuoteSnapshot]):
        self.lock.acquire_write()
        self._data.clear()
        self._data.update(data)
        self.lock.release_write()
        self.update_dt = get_tpe_datetime()

    def _check_updated(self):
        if (
            self.update_dt is None
            or (get_tpe_datetime() - self.update_dt).seconds > self.tolerance
        ):
            raise Exception(
                f"snapshots outdated, previous update time: {self.update_dt}"
            )

    def check_updated(self, codes: List[str] = ["0050", "2330", "2317"]) -> bool:
        if Config.DEBUG:
            return True

        result = True
        try:
            self.lock.acquire_read()
            for code in codes:
                if code in self._data:
                    if (
                        get_tpe_datetime() - self._data[code].dt
                    ).seconds > self.tolerance:
                        result = False
        finally:
            self.lock.release_read()

        return result

    def get_snapshot(self, code: str) -> QuoteSnapshot:
        self._check_updated()
        if code in self._data:
            return self._data[code]
        raise Exception(f"cannot find snapshot: {code}")


class Positions:
    def __init__(self, tolerance: int = 60):
        self._data: Dict[int, Dict[str, Position]] = {}
        self.lock = ReadWriteLock()
        self.update_dt: dt.datetime = None
        self.tolerance = tolerance

    def update(self, data: Dict[int, Dict[str, Position]]):
        self.lock.acquire_write()
        self._data.clear()
        self._data.update(data)
        self.lock.release_write()
        self.update_dt = get_tpe_datetime()

    def _check_updated(self):
        if (
            self.update_dt is None
            or (get_tpe_datetime() - self.update_dt).seconds > self.tolerance
        ):
            raise Exception(
                f"positions outdated, previous update time: {self.update_dt}"
            )

    def check_updated(self) -> bool:
        if (
            self.update_dt is None
            or (get_tpe_datetime() - self.update_dt).seconds > self.tolerance
        ):
            return False
        return True

    def get_position_codes(self) -> List[str]:
        codes = []
        try:
            self.lock.acquire_read()
            for strategy_id in self._data:
                codes.extend(list(self._data[strategy_id]))
        finally:
            self.lock.release_read()
        return codes

    def get_position_strategy_codes(self) -> List[Tuple[int, str]]:
        data = [
            (strategy_id, code) for strategy_id, d0 in self._data.items() for code in d0
        ]
        return data

    def exists(self, strategy_id: int, code: str) -> bool:
        self._check_updated()
        if strategy_id in self._data:
            if code in self._data[strategy_id]:
                return True
        return False

    def get_position(self, strategy_id: int, code: str) -> Position:
        self._check_updated()
        if strategy_id in self._data:
            if code in self._data[strategy_id]:
                return self._data[strategy_id][code]

        raise Exception(f"cannot find position: {strategy_id}, {code}")


class Contracts:
    def __init__(self):
        self._data: Dict[str, Contract] = {}
        self.lock = ReadWriteLock()
        self.update_dt: dt.datetime = None

    def update(self, data: Dict[int, Contract]):
        self.lock.acquire_write()
        self._data.clear()
        self._data.update(data)
        self.lock.release_write()
        self.update_dt = get_tpe_datetime()

    def _check_updated(self, code: str):
        if not self.check_updated([code]):
            raise Exception(f"contract outdated: {code}")

    def check_updated(self, codes: List[str] = ["0050", "2330", "2317"]) -> bool:
        result = True
        try:
            self.lock.acquire_read()
            for code in codes:
                if code in self._data:
                    if self._data[code].update_date != get_tpe_datetime().date():
                        if Config.DEBUG:
                            result = True
                        else:
                            result = False
        finally:
            self.lock.release_read()
        return result

    def exists(self, code: str) -> bool:
        return code in self._data

    def get_contract(self, code: str) -> Contract:
        self._check_updated(code)
        if code in self._data:
            return self._data[code]
        raise Exception(f"cannot find contract: {code}")


class ComingDividends:
    def __init__(self):
        self._data: Dict[str, ComingDividend] = {}
        self.lock = ReadWriteLock()
        self.update_dt: dt.datetime = None

    def update(self, data: Dict[int, ComingDividend]):
        self.lock.acquire_write()
        self._data.clear()
        self._data.update(data)
        self.lock.release_write()
        self.update_dt = get_tpe_datetime()

    def _check_updated(self):
        if not self.check_updated():
            raise Exception(
                f"coming_dividend outdated, previous update time: {self.update_dt}"
            )

    def check_updated(self) -> bool:
        if self.update_dt is None or (
            get_tpe_datetime().date() != self.update_dt.date()
        ):
            return False
        return True

    def exists(self, code: str) -> bool:
        return code in self._data

    def get_coming_dividend(self, code: str) -> ComingDividend:
        self._check_updated()
        if code in self._data:
            return self._data[code]
        raise Exception(f"cannot find coming_dividend: {code}")


class TradingDates:
    def __init__(self):
        self._data: List[dt.date] = []
        self.lock = ReadWriteLock()
        self.update_dt: dt.datetime = None
        self.today = None
        self._is_trading_date = False

    def update(self, data: List[dt.date]):
        self.lock.acquire_write()
        self._data.clear()
        self._data.extend(data)
        self.lock.release_write()
        self.update_dt = get_tpe_datetime()
        self.today = self.update_dt.date()
        if self.today in self._data:
            self._is_trading_date = True
        else:
            self._is_trading_date = False

    def is_trading_date(self) -> bool:
        if Config.DEBUG:
            return True
        self._check_updated()
        return self._is_trading_date

    def _check_updated(self):
        if not self.check_updated():
            raise Exception(
                f"trading_dates outdated, previous update time: {self.update_dt}"
            )

    def check_updated(self) -> bool:
        if self.update_dt is None or (
            get_tpe_datetime().date() != self.update_dt.date()
        ):
            return False
        return True

    def get_next_n_trading_date(
        self, bench_date: dt.date = None, days: int = 0
    ) -> dt.date:
        self._check_updated()
        if bench_date:
            tdate = bench_date
        else:
            tdate = self.today

        self.lock.acquire_read()
        target_date: dt.date = None
        if tdate in self._data:
            idx = self._data.index(tdate)
            if idx + days < len(self._data):
                target_date = self._data[idx + days]
        self.lock.release_read()

        if target_date is None:
            if Config.DEBUG:
                return self.get_next_n_trading_date(
                    tdate - dt.timedelta(days=1), days=days
                )

            raise Exception(f"{tdate} not in trading dates or days out of range")

        return target_date
