from bunny_order.models import SF31Order

class DailyTransactionAmountExceeded(Exception):
    def __init__(self, order: SF31Order):
        super().__init__(order)