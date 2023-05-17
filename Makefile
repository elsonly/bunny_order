build-proto:
	python -m grpc_tools.protoc -Ibunny_order/grpc_relay --python_out=bunny_order/grpc_relay --pyi_out=bunny_order/grpc_relay --grpc_python_out=bunny_order/grpc_relay bunny_order/grpc_relay/relay.proto

run-grpc-server:
	python -m bunny_order.grpc_relay.relay_server

run-grpc-client:
 	python -m bunny_order.grpc_relay.relay_client

run-fastapi-server:
	uvicorn bunny_order.fastapi_relay.main:app --reload --host 0.0.0.0 --port 8087

run-indirect:
	python -m bunny_order.indirect_relay