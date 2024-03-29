# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: components/auth/proto/permissions.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from components.auth.proto import realms_pb2 as components_dot_auth_dot_proto_dot_realms__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='components/auth/proto/permissions.proto',
  package='components.auth.permissions',
  syntax='proto3',
  serialized_options=b'Z6go.chromium.org/luci/auth_service/internal/permissions',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\'components/auth/proto/permissions.proto\x12\x1b\x63omponents.auth.permissions\x1a\"components/auth/proto/realms.proto\"J\n\x0fPermissionsList\x12\x37\n\x0bpermissions\x18\x01 \x03(\x0b\x32\".components.auth.realms.PermissionB8Z6go.chromium.org/luci/auth_service/internal/permissionsb\x06proto3'
  ,
  dependencies=[components_dot_auth_dot_proto_dot_realms__pb2.DESCRIPTOR,])




_PERMISSIONSLIST = _descriptor.Descriptor(
  name='PermissionsList',
  full_name='components.auth.permissions.PermissionsList',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='permissions', full_name='components.auth.permissions.PermissionsList.permissions', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=108,
  serialized_end=182,
)

_PERMISSIONSLIST.fields_by_name['permissions'].message_type = components_dot_auth_dot_proto_dot_realms__pb2._PERMISSION
DESCRIPTOR.message_types_by_name['PermissionsList'] = _PERMISSIONSLIST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

PermissionsList = _reflection.GeneratedProtocolMessageType('PermissionsList', (_message.Message,), {
  'DESCRIPTOR' : _PERMISSIONSLIST,
  '__module__' : 'components.auth.proto.permissions_pb2'
  # @@protoc_insertion_point(class_scope:components.auth.permissions.PermissionsList)
  })
_sym_db.RegisterMessage(PermissionsList)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
