import logging

import grpc
from bunny_order.grpc_relay.relay_pb2 import OrderRequest, OrderResponse
from bunny_order.grpc_relay import relay_pb2_grpc
from bunny_order.config import Config


def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    print("Will try to greet world ...")
    with grpc.insecure_channel(f"{Config.GRPC_HOST}:{Config.GRPC_PORT}") as channel:
        stub = relay_pb2_grpc.RelayStub(channel)
        response: OrderResponse = stub.PlaceOrder(OrderRequest(name="you"))
    print("Greeter client received: " + response.message)


if __name__ == "__main__":
    logging.basicConfig()
    run()
