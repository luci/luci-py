# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: go.chromium.org/luci/buildbucket/proto/notification.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from bb.go.chromium.org.luci.buildbucket.proto import build_pb2 as go_dot_chromium_dot_org_dot_luci_dot_buildbucket_dot_proto_dot_build__pb2
from bb.go.chromium.org.luci.buildbucket.proto import common_pb2 as go_dot_chromium_dot_org_dot_luci_dot_buildbucket_dot_proto_dot_common__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='go.chromium.org/luci/buildbucket/proto/notification.proto',
  package='buildbucket.v2',
  syntax='proto3',
  serialized_options=b'Z4go.chromium.org/luci/buildbucket/proto;buildbucketpb',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n9go.chromium.org/luci/buildbucket/proto/notification.proto\x12\x0e\x62uildbucket.v2\x1a\x32go.chromium.org/luci/buildbucket/proto/build.proto\x1a\x33go.chromium.org/luci/buildbucket/proto/common.proto\"=\n\x12NotificationConfig\x12\x14\n\x0cpubsub_topic\x18\x01 \x01(\t\x12\x11\n\tuser_data\x18\x02 \x01(\x0c\"\x84\x01\n\x0e\x42uildsV2PubSub\x12$\n\x05\x62uild\x18\x01 \x01(\x0b\x32\x15.buildbucket.v2.Build\x12\x1a\n\x12\x62uild_large_fields\x18\x02 \x01(\x0c\x12\x30\n\x0b\x63ompression\x18\x03 \x01(\x0e\x32\x1b.buildbucket.v2.Compression\"Y\n\x0ePubSubCallBack\x12\x34\n\x0c\x62uild_pubsub\x18\x01 \x01(\x0b\x32\x1e.buildbucket.v2.BuildsV2PubSub\x12\x11\n\tuser_data\x18\x02 \x01(\x0c\x42\x36Z4go.chromium.org/luci/buildbucket/proto;buildbucketpbb\x06proto3'
  ,
  dependencies=[go_dot_chromium_dot_org_dot_luci_dot_buildbucket_dot_proto_dot_build__pb2.DESCRIPTOR,go_dot_chromium_dot_org_dot_luci_dot_buildbucket_dot_proto_dot_common__pb2.DESCRIPTOR,])




_NOTIFICATIONCONFIG = _descriptor.Descriptor(
  name='NotificationConfig',
  full_name='buildbucket.v2.NotificationConfig',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='pubsub_topic', full_name='buildbucket.v2.NotificationConfig.pubsub_topic', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='user_data', full_name='buildbucket.v2.NotificationConfig.user_data', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=182,
  serialized_end=243,
)


_BUILDSV2PUBSUB = _descriptor.Descriptor(
  name='BuildsV2PubSub',
  full_name='buildbucket.v2.BuildsV2PubSub',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='build', full_name='buildbucket.v2.BuildsV2PubSub.build', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='build_large_fields', full_name='buildbucket.v2.BuildsV2PubSub.build_large_fields', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='compression', full_name='buildbucket.v2.BuildsV2PubSub.compression', index=2,
      number=3, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=246,
  serialized_end=378,
)


_PUBSUBCALLBACK = _descriptor.Descriptor(
  name='PubSubCallBack',
  full_name='buildbucket.v2.PubSubCallBack',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='build_pubsub', full_name='buildbucket.v2.PubSubCallBack.build_pubsub', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='user_data', full_name='buildbucket.v2.PubSubCallBack.user_data', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=380,
  serialized_end=469,
)

_BUILDSV2PUBSUB.fields_by_name['build'].message_type = go_dot_chromium_dot_org_dot_luci_dot_buildbucket_dot_proto_dot_build__pb2._BUILD
_BUILDSV2PUBSUB.fields_by_name['compression'].enum_type = go_dot_chromium_dot_org_dot_luci_dot_buildbucket_dot_proto_dot_common__pb2._COMPRESSION
_PUBSUBCALLBACK.fields_by_name['build_pubsub'].message_type = _BUILDSV2PUBSUB
DESCRIPTOR.message_types_by_name['NotificationConfig'] = _NOTIFICATIONCONFIG
DESCRIPTOR.message_types_by_name['BuildsV2PubSub'] = _BUILDSV2PUBSUB
DESCRIPTOR.message_types_by_name['PubSubCallBack'] = _PUBSUBCALLBACK
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

NotificationConfig = _reflection.GeneratedProtocolMessageType('NotificationConfig', (_message.Message,), {
  'DESCRIPTOR' : _NOTIFICATIONCONFIG,
  '__module__' : 'go.chromium.org.luci.buildbucket.proto.notification_pb2'
  # @@protoc_insertion_point(class_scope:buildbucket.v2.NotificationConfig)
  })
_sym_db.RegisterMessage(NotificationConfig)

BuildsV2PubSub = _reflection.GeneratedProtocolMessageType('BuildsV2PubSub', (_message.Message,), {
  'DESCRIPTOR' : _BUILDSV2PUBSUB,
  '__module__' : 'go.chromium.org.luci.buildbucket.proto.notification_pb2'
  # @@protoc_insertion_point(class_scope:buildbucket.v2.BuildsV2PubSub)
  })
_sym_db.RegisterMessage(BuildsV2PubSub)

PubSubCallBack = _reflection.GeneratedProtocolMessageType('PubSubCallBack', (_message.Message,), {
  'DESCRIPTOR' : _PUBSUBCALLBACK,
  '__module__' : 'go.chromium.org.luci.buildbucket.proto.notification_pb2'
  # @@protoc_insertion_point(class_scope:buildbucket.v2.PubSubCallBack)
  })
_sym_db.RegisterMessage(PubSubCallBack)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
