from bunny_order.order_manager import OrderManager

if __name__ == "__main__":
    om = OrderManager(enable_trade=True)
    om.run()
