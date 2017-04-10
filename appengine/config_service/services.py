# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Provides info about registered luci services."""

from google.appengine.ext import ndb

from components import config
from components import net
from components import utils
from components.config.proto import service_config_pb2

import common
import storage
import validation


class DynamicMetadataError(Exception):
  """Raised when a service metadata endpoint response is bad."""


class ServiceNotFoundError(Exception):
  """Raised when a service is not found."""


@ndb.tasklet
def get_services_async():
  """Returns a list of registered luci services.

  The list is stored in services/luci-config:services.cfg. Never returns None.
  Cached.

  Returns:
    A list of service_config_pb2.Service.
  """
  cfg = yield storage.get_self_config_async(
      common.SERVICES_REGISTRY_FILENAME, service_config_pb2.ServicesCfg)
  raise ndb.Return(cfg.services or [])


@ndb.tasklet
def get_service_async(service_id):
  """Returns a service config by id.

  Returns:
    service_config_pb2.Service, or None if not found.
  """
  services = yield get_services_async()
  for service in services:
    if service.id == service_id:
      raise ndb.Return(service)


def _dict_to_dynamic_metadata(data):
  validation.validate_service_dynamic_metadata_blob(
      data,
      config.validation.Context.raise_on_error(exc_type=DynamicMetadataError))

  metadata = service_config_pb2.ServiceDynamicMetadata()
  validation_meta = data.get('validation')
  if validation_meta:
    metadata.validation.url = validation_meta['url']
    for p in validation_meta.get('patterns', []):
      pattern = metadata.validation.patterns.add()
      pattern.config_set=p['config_set']
      pattern.path=p['path']
  return metadata


@ndb.tasklet
def get_metadata_async(service_id):
  """Returns service dynamic metadata.

  Memcaches results for 1 min. Never returns None.

  Raises:
    ServiceNotFoundError if service |service_id| is not found.
    DynamicMetadataError if metadata endpoint response is bad.
  """
  cache_key = 'get_metadata(%r)' % service_id
  ctx = ndb.get_context()
  cached = yield ctx.memcache_get(cache_key)
  if cached:
    msg = service_config_pb2.ServiceDynamicMetadata()
    msg.ParseFromString(cached)
    raise ndb.Return(msg)

  service = yield get_service_async(service_id)
  if service is None:
    raise ServiceNotFoundError('Service "%s" not found', service_id)

  if not service.metadata_url:
    raise ndb.Return(service_config_pb2.ServiceDynamicMetadata())

  try:
    res = yield net.json_request_async(
        service.metadata_url, scopes=net.EMAIL_SCOPE)
  except net.Error as ex:
    raise DynamicMetadataError('Net error: %s' % ex.message)
  msg = _dict_to_dynamic_metadata(res)
  yield ctx.memcache_set(cache_key, msg.SerializeToString(), time=60)
  raise ndb.Return(msg)
