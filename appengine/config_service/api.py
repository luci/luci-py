# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import logging

from protorpc import messages
from protorpc import message_types
from protorpc import remote
import endpoints

from components import auth
from components import utils

import acl
import projects
import storage


# This is used by endpoints indirectly.
package = 'luci-config'


class Project(messages.Message):
  # Unique luci project id from services/luci-config:projects.cfg
  id = messages.StringField(1, required=True)
  # Project name from projects/<project_id>:project.cfg
  name = messages.StringField(2)
  repo_type = messages.EnumField(projects.RepositoryType, 3, required=True)
  repo_url = messages.StringField(4, required=True)


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

  ##############################################################################
  # endpoint: get_config

  GET_CONFIG_REQUEST_RESOURCE_CONTAINER = endpoints.ResourceContainer(
      message_types.VoidMessage,
      config_set=messages.StringField(1, required=True),
      path=messages.StringField(2, required=True),
      revision=messages.StringField(3),
      hash_only=messages.BooleanField(4),
  )

  class GetConfigResponseMessage(messages.Message):
    revision = messages.StringField(1, required=True)
    content_hash = messages.StringField(2, required=True)
    # If request.only_hash is not set to True, the contents of the
    # config file.
    content = messages.BytesField(3)

  @auth.endpoints_method(
      GET_CONFIG_REQUEST_RESOURCE_CONTAINER,
      GetConfigResponseMessage,
      http_method='GET',
      path='config_sets/{config_set}/config/{path}')
  def get_config(self, request):
    """Gets a config file."""
    res = self.GetConfigResponseMessage()

    try:
      has_access = acl.can_read_config_set(
          request.config_set, headers=self.request_state.headers)
    except ValueError:
      raise endpoints.BadRequestException(
          'Invalid config set: %s' % request.config_set)

    if not has_access:
      logging.warning(
          '%s does not have access to %s',
          auth.get_current_identity().to_bytes(),
          request.config_set)
      raise_config_not_found()

    res.revision, res.content_hash = storage.get_config_hash(
        request.config_set, request.path, revision=request.revision)
    if not res.content_hash:
      raise_config_not_found()

    if not request.hash_only:
      res.content = storage.get_config_by_hash(res.content_hash)
      if not res.content:
        logging.warning(
            'Config hash is found, but the blob is not.\n'
            'File: "%s:%s:%s". Hash: %s', request.config_set,
            request.revision, request.path, res.content_hash)
        raise_config_not_found()
    return res

  ##############################################################################
  # endpoint: get_config_by_hash

  GET_CONFIG_BY_HASH_REQUEST_RESOURCE_CONTAINER = endpoints.ResourceContainer(
      message_types.VoidMessage,
      content_hash=messages.StringField(1, required=True),
  )

  class GetConfigByHashResponseMessage(messages.Message):
    content = messages.BytesField(1, required=True)

  @auth.endpoints_method(
      GET_CONFIG_BY_HASH_REQUEST_RESOURCE_CONTAINER,
      GetConfigByHashResponseMessage,
      http_method='GET',
      path='config/{content_hash}')
  def get_config_by_hash(self, request):
    """Gets a config file by its hash."""
    res = self.GetConfigByHashResponseMessage(
        content=storage.get_config_by_hash(request.content_hash))
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
  def get_projects(self, request):  # pylint: disable=W0613
    """Gets list of registered projects.

    The project list is stored in services/luci-config:projects.cfg.
    """
    if not acl.can_read_project_list():
      raise endpoints.ForbiddenException()
    return self.GetProjectsResponseMessage(projects=get_projects())

  ##############################################################################
  # endpoint: get_refs

  GET_REFS_REQUEST_RESOURCE_CONTAINER = endpoints.ResourceContainer(
      message_types.VoidMessage,
      project_id=messages.StringField(1, required=True),
  )

  class GetRefsResponseMessage(messages.Message):
    class Ref(messages.Message):
      name = messages.StringField(1)
    refs = messages.MessageField(Ref, 1, repeated=True)

  @auth.endpoints_method(
      GET_REFS_REQUEST_RESOURCE_CONTAINER,
      GetRefsResponseMessage,
      http_method='GET',
      path='projects/{project_id}/refs')
  def get_refs(self, request):
    """Gets list of refs of a project."""
    if not acl.can_read_project_config(request.project_id):
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
  def get_project_configs(self, request):
    """Gets configs in all project config sets."""

    def iter_project_config_sets():
      for project in get_projects():
        yield 'projects/%s' % project.id

    return get_config_multi(
        iter_project_config_sets(), request.path, request.hashes_only)

  ##############################################################################
  # endpoint: get_ref_configs

  @auth.endpoints_method(
      GET_CONFIG_MULTI_REQUEST_RESOURCE_CONTAINER,
      GetConfigMultiResponseMessage,
      http_method='GET',
      path='configs/refs/{path}')
  def get_ref_configs(self, request):
    """Gets configs in all ref config sets."""

    def iter_ref_config_sets():
      for project in get_projects():
        for ref_name in get_ref_names(project.id):
          yield 'projects/%s/%s' % (project.id, ref_name)

    return get_config_multi(
        iter_ref_config_sets(), request.path, request.hashes_only)


@utils.memcache('projects_with_details', time=60)  # 1 min.
def get_projects():
  """Returns list of projects with metadata and repo info.

  Does not return projects that have no repo information. It might happen due
  to eventual consistency.

  Caches results in main memory for 10 min.
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


@utils.memcache('ref_names', ['project_id'], time=5*60)  # 5 min.
def get_ref_names(project_id):
  """Returns list of ref names for a project. Caches results."""
  assert project_id
  refs = projects.get_refs(project_id)
  if refs is None:
    # Project does not exist
    return None
  return [ref.name for ref in refs]


def get_config_multi(config_sets, path, hashes_only):
  """Returns configs at |path| in all config sets.

  Returns empty config list if requester does not have project access.
  """
  if not acl.has_project_access():
    raise endpoints.ForbiddenException()

  res = GetConfigMultiResponseMessage()
  configs = storage.get_latest_multi(config_sets, path, hashes_only)
  for config in configs:
    if not hashes_only and config.get('content') is None:
      logging.error(
          'Blob %s referenced from %s:%s:%s was not found',
          config['content_hash'],
          config['config_set'],
          config['revision'],
          path)
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
