# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: relay.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0brelay.proto\x12\x05relay"\x1c\n\x0cOrderRequest\x12\x0c\n\x04name\x18\x01 \x01(\t" \n\rOrderResponse\x12\x0f\n\x07message\x18\x01 \x01(\t2~\n\x05Relay\x12\x39\n\nPlaceOrder\x12\x13.relay.OrderRequest\x1a\x14.relay.OrderResponse"\x00\x12:\n\x0b\x43\x61ncelOrder\x12\x13.relay.OrderRequest\x1a\x14.relay.OrderResponse"\x00\x62\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "relay_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _ORDERREQUEST._serialized_start = 22
    _ORDERREQUEST._serialized_end = 50
    _ORDERRESPONSE._serialized_start = 52
    _ORDERRESPONSE._serialized_end = 84
    _RELAY._serialized_start = 86
    _RELAY._serialized_end = 212
# @@protoc_insertion_point(module_scope)
