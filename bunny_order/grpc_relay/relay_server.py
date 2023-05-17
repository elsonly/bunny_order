from concurrent import futures
import logging

import grpc
from bunny_order.grpc_relay.relay_pb2 import OrderRequest, OrderResponse
from bunny_order.grpc_relay import relay_pb2_grpc
from bunny_order.config import Config


class Relay(relay_pb2_grpc.RelayServicer):
    def PlaceOrder(self, request: OrderRequest, context):
        return OrderResponse(message="Hello, %s!" % request.name)


def serve():
    port = Config.GRPC_PORT
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    relay_pb2_grpc.add_RelayServicer_to_server(Relay(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
