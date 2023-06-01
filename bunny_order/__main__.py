from bunny_order.engine import Engine
from bunny_order.config import Config

if __name__ == "__main__":
    engine = Engine(
        debug=Config.DEBUG,
        sync_interval=5,
    )
    engine.run()
