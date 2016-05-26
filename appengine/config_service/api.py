# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import logging

from google.appengine.api import memcache
from google.appengine.ext import ndb
from protorpc import messages
from protorpc import message_types
from protorpc import remote
import endpoints

from components import auth
from components import utils
from components.config import endpoint as cfg_endpoint

import acl
import gitiles_import
import projects
import storage
import validation


# This is used by endpoints indirectly.
package = 'luci-config'


class Project(messages.Message):
  # Unique luci project id from services/luci-config:projects.cfg
  id = messages.StringField(1, required=True)
  # Project name from projects/<project_id>:project.cfg
  name = messages.StringField(2)
  repo_type = messages.EnumField(projects.RepositoryType, 3, required=True)
  repo_url = messages.StringField(4, required=True)


class Revision(messages.Message):
  id = messages.StringField(1)
  url = messages.StringField(2)
  timestamp = messages.IntegerField(3)
  committer_email = messages.StringField(4)


class ConfigSet(messages.Message):
  """Describes a config set."""

  class ImportAttempt(messages.Message):
    timestamp = messages.IntegerField(1)
    revision = messages.MessageField(Revision, 2)
    success = messages.BooleanField(3)
    message = messages.StringField(4)
    validation_messages = messages.MessageField(
        cfg_endpoint.ValidationMessage, 5, repeated=True)

  config_set = messages.StringField(1, required=True)
  location = messages.StringField(2)
  revision = messages.MessageField(Revision, 3)
  last_import_attempt = messages.MessageField(ImportAttempt, 4)


def attempt_to_msg(entity):
  if entity is None:
    return None
  return ConfigSet.ImportAttempt(
    timestamp=utils.datetime_to_timestamp(entity.time),
    revision=Revision(
        id=entity.revision.id,
        url=entity.revision.url,
        timestamp=utils.datetime_to_timestamp(entity.revision.time),
        committer_email=entity.revision.committer_email,
    ) if entity.revision else None,
    success=entity.success,
    message=entity.message,
    validation_messages=[
      cfg_endpoint.ValidationMessage(severity=m.severity, text=m.text)
      for m in entity.validation_messages
    ],
  )


GET_CONFIG_MULTI_REQUEST_RESOURCE_CONTAINER = endpoints.ResourceContainer(
    message_types.VoidMessage,
    path=messages.StringField(1, required=True),
    # If True, response.content will be None.
    hashes_only=messages.BooleanField(2, default=False),
)


class GetConfigMultiResponseMessage(messages.Message):
  class ConfigEntry(messages.Message):
    config_set = messages.StringField(1, required=True)
    revision = messages.StringField(2, required=True)
    content_hash = messages.StringField(3, required=True)
    # None if request.hash_only is True
    content = messages.BytesField(4)
  configs = messages.MessageField(ConfigEntry, 1, repeated=True)


@auth.endpoints_api(name='config', version='v1', title='Configuration Service')
class ConfigApi(remote.Service):
  """API to access configurations."""

  def can_read_config_set(self, config_set):
    try:
      return acl.can_read_config_set(config_set)
    except ValueError:
      raise endpoints.BadRequestException('Invalid config set: %s' % config_set)

  ##############################################################################
  # endpoint: get_mapping

  class GetMappingResponseMessage(messages.Message):
    class Mapping(messages.Message):
      config_set = messages.StringField(1, required=True)
      location = messages.StringField(2)
    mappings = messages.MessageField(Mapping, 1, repeated=True)

  @auth.endpoints_method(
      endpoints.ResourceContainer(
          message_types.VoidMessage,
          config_set=messages.StringField(1),
      ),
      GetMappingResponseMessage,
      http_method='GET',
      path='mapping')
  @auth.public # ACL check inside
  def get_mapping(self, request):
    """DEPRECATED. Use get_config_sets."""
    if request.config_set and not self.can_read_config_set(request.config_set):
      raise endpoints.ForbiddenException()

    config_sets = storage.get_config_sets_async(
        config_set=request.config_set).get_result()
    return self.GetMappingResponseMessage(
        mappings=[
          self.GetMappingResponseMessage.Mapping(
              config_set=cs.key.id(), location=cs.location)
          for cs in config_sets
          if self.can_read_config_set(cs.key.id())
        ]
    )

  ##############################################################################
  # endpoint: get_config_set

  class GetConfigSetsResponseMessage(messages.Message):
    config_sets = messages.MessageField(ConfigSet, 1, repeated=True)

  @auth.endpoints_method(
    endpoints.ResourceContainer(
        message_types.VoidMessage,
        config_set=messages.StringField(1),
        include_last_import_attempt=messages.BooleanField(2),
    ),
    GetConfigSetsResponseMessage,
    http_method='GET',
    path='config-sets')
  @auth.public # ACL check inside
  def get_config_sets(self, request):
    """Returns config sets."""
    if request.config_set and not self.can_read_config_set(request.config_set):
      raise endpoints.ForbiddenException()

    config_sets = storage.get_config_sets_async(
        config_set=request.config_set).get_result()

    if request.include_last_import_attempt:
      attempts = ndb.get_multi([
        storage.last_import_attempt_key(cs.key.id()) for cs in config_sets
      ])
    else:
      attempts = [None] * len(config_sets)

    res = self.GetConfigSetsResponseMessage()
    for cs, attempt in zip(config_sets, attempts):
      if self.can_read_config_set(cs.key.id()):
        timestamp = None
        if cs.latest_revision_time:
          timestamp = utils.datetime_to_timestamp(cs.latest_revision_time)
        res.config_sets.append(ConfigSet(
            config_set=cs.key.id(),
            location=cs.location,
            revision=Revision(
                id=cs.latest_revision,
                url=cs.latest_revision_url,
                timestamp=timestamp,
                committer_email=cs.latest_revision_committer_email,
            ),
            last_import_attempt=attempt_to_msg(attempt),
        ))
    return res

  ##############################################################################
  # endpoint: get_config

  class GetConfigResponseMessage(messages.Message):
    revision = messages.StringField(1, required=True)
    content_hash = messages.StringField(2, required=True)
    # If request.only_hash is not set to True, the contents of the
    # config file.
    content = messages.BytesField(3)

  @auth.endpoints_method(
      endpoints.ResourceContainer(
          message_types.VoidMessage,
          config_set=messages.StringField(1, required=True),
          path=messages.StringField(2, required=True),
          revision=messages.StringField(3),
          hash_only=messages.BooleanField(4),
      ),
      GetConfigResponseMessage,
      http_method='GET',
      path='config_sets/{config_set}/config/{path}')
  @auth.public # ACL check inside
  def get_config(self, request):
    """Gets a config file."""
    try:
      validation.validate_config_set(request.config_set)
      validation.validate_path(request.path)
    except ValueError as ex:
      raise endpoints.BadRequestException(ex.message)
    res = self.GetConfigResponseMessage()

    if not self.can_read_config_set(request.config_set):
      logging.warning(
          '%s does not have access to %s',
          auth.get_current_identity().to_bytes(),
          request.config_set)
      raise_config_not_found()

    res.revision, res.content_hash = (
        storage.get_config_hash_async(
            request.config_set, request.path, revision=request.revision)
        .get_result())
    if not res.content_hash:
      raise_config_not_found()

    if not request.hash_only:
      res.content = (
          storage.get_config_by_hash_async(res.content_hash).get_result())
      if not res.content:
        logging.warning(
            'Config hash is found, but the blob is not.\n'
            'File: "%s:%s:%s". Hash: %s', request.config_set,
            request.revision, request.path, res.content_hash)
        raise_config_not_found()
    return res

  ##############################################################################
  # endpoint: get_config_by_hash

  class GetConfigByHashResponseMessage(messages.Message):
    content = messages.BytesField(1, required=True)

  @auth.endpoints_method(
      endpoints.ResourceContainer(
          message_types.VoidMessage,
          content_hash=messages.StringField(1, required=True),
      ),
      GetConfigByHashResponseMessage,
      http_method='GET',
      path='config/{content_hash}')
  @auth.public
  def get_config_by_hash(self, request):
    """Gets a config file by its hash."""
    res = self.GetConfigByHashResponseMessage(
        content=storage.get_config_by_hash_async(
            request.content_hash).get_result())
    if not res.content:
      raise_config_not_found()
    return res

  ##############################################################################
  # endpoint: get_projects

  class GetProjectsResponseMessage(messages.Message):
    projects = messages.MessageField(Project, 1, repeated=True)

  @auth.endpoints_method(
      message_types.VoidMessage,
      GetProjectsResponseMessage,
      http_method='GET',
      path='projects')
  @auth.public # ACL check inside
  def get_projects(self, request):  # pylint: disable=W0613
    """Gets list of registered projects.

    The project list is stored in services/luci-config:projects.cfg.
    """
    return self.GetProjectsResponseMessage(
        projects=[p for p in get_projects() if acl.has_project_access(p.id)],
    )

  ##############################################################################
  # endpoint: get_refs

  class GetRefsResponseMessage(messages.Message):
    class Ref(messages.Message):
      name = messages.StringField(1)
    refs = messages.MessageField(Ref, 1, repeated=True)

  @auth.endpoints_method(
      endpoints.ResourceContainer(
          message_types.VoidMessage,
          project_id=messages.StringField(1, required=True),
      ),
      GetRefsResponseMessage,
      http_method='GET',
      path='projects/{project_id}/refs')
  @auth.public # ACL check inside
  def get_refs(self, request):
    """Gets list of refs of a project."""
    if not acl.has_project_access(request.project_id):
      raise endpoints.NotFoundException()
    ref_names = get_ref_names(request.project_id)
    if ref_names is None:
      # Project not found
      raise endpoints.NotFoundException()
    res = self.GetRefsResponseMessage()
    res.refs = [res.Ref(name=ref) for ref in ref_names]
    return res

  ##############################################################################
  # endpoint: get_project_configs

  @auth.endpoints_method(
      GET_CONFIG_MULTI_REQUEST_RESOURCE_CONTAINER,
      GetConfigMultiResponseMessage,
      http_method='GET',
      path='configs/projects/{path}')
  @auth.public # ACL check inside
  def get_project_configs(self, request):
    """Gets configs in all project config sets."""
    try:
      validation.validate_path(request.path)
    except ValueError as ex:
      raise endpoints.BadRequestException(ex.message)

    return get_config_multi('projects', request.path, request.hashes_only)

  ##############################################################################
  # endpoint: get_ref_configs

  @auth.endpoints_method(
      GET_CONFIG_MULTI_REQUEST_RESOURCE_CONTAINER,
      GetConfigMultiResponseMessage,
      http_method='GET',
      path='configs/refs/{path}')
  @auth.public # ACL check inside
  def get_ref_configs(self, request):
    """Gets configs in all ref config sets."""
    try:
      validation.validate_path(request.path)
    except ValueError as ex:
      raise endpoints.BadRequestException(ex.message)

    return get_config_multi('refs', request.path, request.hashes_only)

  ##############################################################################
  # endpoint: reimport

  @auth.endpoints_method(
    endpoints.ResourceContainer(
        message_types.VoidMessage,
        config_set=messages.StringField(1, required=True)
    ),
    message_types.VoidMessage,
    http_method='POST',
    path='reimport')
  @auth.public # ACL check inside
  def reimport(self, request):
    """Reimports a config set."""
    if not auth.is_admin():
      raise endpoints.ForbiddenException('Only admins are allowed to do this')
    # Assume it is Gitiles.
    try:
      gitiles_import.import_config_set(request.config_set)
      return message_types.VoidMessage()
    except gitiles_import.NotFoundError as e:
      raise endpoints.NotFoundException(e.message)
    except ValueError as e:
      raise endpoints.BadRequestException(e.message)
    except gitiles_import.Error as e:
      raise endpoints.InternalServerErrorException(e.message)


@utils.memcache('projects_with_details', time=60)  # 1 min.
def get_projects():
  """Returns list of projects with metadata and repo info.

  Does not return projects that have no repo information. It might happen due
  to eventual consistency.

  Caches results in memcache for 10 min.
  """
  result = []
  for p in projects.get_projects():
    repo_type, repo_url = projects.get_repo(p.id)
    if repo_type is None:
      # Not yet consistent.
      continue
    metadata = projects.get_metadata(p.id)
    result.append(Project(
        id=p.id,
        name=metadata.name or None,
        repo_type=repo_type,
        repo_url=repo_url,
    ))
  return result


@utils.memcache('ref_names', ['project_id'], time=60)  # 1 min.
def get_ref_names(project_id):
  """Returns list of ref names for a project. Caches results."""
  assert project_id
  refs = projects.get_refs(project_id)
  if refs is None:
    # Project does not exist
    return None
  return [ref.name for ref in refs]


def get_config_sets_from_scope(scope):
  """Returns list of config sets from 'projects' or 'refs'."""
  assert scope in ('projects', 'refs'), scope
  for project in get_projects():
    if scope == 'projects':
      yield 'projects/%s' % project.id
    else:
      for ref_name in get_ref_names(project.id):
        yield 'projects/%s/%s' % (project.id, ref_name)


def get_config_multi(scope, path, hashes_only):
  """Returns configs at |path| in all config sets.

  scope can be 'projects' or 'refs'.

  Returns empty config list if requester does not have project access.
  """
  assert scope in ('projects', 'refs'), scope
  cache_key = (
    '%s%s:%s' % (scope, ',hashes_only' if hashes_only else '', path))
  configs = memcache.get(cache_key)
  if configs is None:
    configs = storage.get_latest_multi_async(
        get_config_sets_from_scope(scope), path, hashes_only).get_result()
    for config in configs:
      if not hashes_only and config.get('content') is None:
        logging.error(
            'Blob %s referenced from %s:%s:%s was not found',
            config['content_hash'],
            config['config_set'],
            config['revision'],
            path)
    try:
      memcache.add(cache_key, configs, time=60)
    except ValueError:
      logging.exception('%s:%s configs are too big for memcache', scope, path)

  res = GetConfigMultiResponseMessage()
  for config in configs:
    if not acl.can_read_config_set(config['config_set']):
      continue
    if not hashes_only and config.get('content') is None:
      continue
    res.configs.append(res.ConfigEntry(
        config_set=config['config_set'],
        revision=config['revision'],
        content_hash=config['content_hash'],
        content=config.get('content'),
    ))
  return res


def raise_config_not_found():
  raise endpoints.NotFoundException('The requested config is not found')
