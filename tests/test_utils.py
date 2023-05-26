import pytest
from decimal import Decimal

from bunny_order.utils import adjust_price_for_tick_unit


@pytest.mark.parametrize(
    "price, expected",
    [
        (Decimal("9.011"), Decimal("9.01")),
        (Decimal("9.015"), Decimal("9.02")),
        (Decimal("9.019"), Decimal("9.02")),
        (Decimal("10.021"), Decimal("10.00")),
        (Decimal("10.025"), Decimal("10.05")),
        (Decimal("10.026"), Decimal("10.05")),
        (Decimal("50.11"), Decimal("50.10")),
        (Decimal("50.15"), Decimal("50.20")),
        (Decimal("50.16"), Decimal("50.20")),
        (Decimal("100.16"), Decimal("100.00")),
        (Decimal("100.49"), Decimal("100.50")),
        (Decimal("100.51"), Decimal("100.50")),
        (Decimal("500.40"), Decimal("500.00")),
        (Decimal("500.49"), Decimal("500.00")),
        (Decimal("500.50"), Decimal("501.00")),
        (Decimal("1002.4"), Decimal("1000")),
        (Decimal("1002.5"), Decimal("1005")),
        (Decimal("1004.0"), Decimal("1005")),

    ],
)
def test_adjust_price_for_tick_unit(price: Decimal, expected: Decimal):
    assert adjust_price_for_tick_unit(price) == expected
