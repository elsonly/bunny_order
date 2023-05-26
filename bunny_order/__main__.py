from bunny_order.engine import Engine
from bunny_order.config import Config

if __name__ == "__main__":
    engine = Engine(
        debug=Config.DEBUG,
        sync_interval=5,
        trade_start_time=Config.ENGINE_TRADE_START_TIME,
        trade_end_time=Config.ENGINE_TRADE_END_TIME,
    )
    engine.run()
