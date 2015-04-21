# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Provides info about projects (service tenants)."""

import logging

from components import utils

from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop
from protorpc import messages

from proto import project_config_pb2
from proto import service_config_pb2
import common
import storage


DEFAULT_REF_CFG = project_config_pb2.RefsCfg(
    refs=[project_config_pb2.RefsCfg.Ref(name='refs/heads/master')])


class RepositoryType(messages.Enum):
  GITILES = 1


class ProjectImportInfo(ndb.Model):
  """Contains info how a project was imported.

  Entity key:
    Id is project id from the project registry. Has no parent.
  """
  created_ts = ndb.DateTimeProperty(auto_now_add=True)
  repo_type = msgprop.EnumProperty(RepositoryType, required=True)
  repo_url = ndb.StringProperty(required=True)


@ndb.transactional
def update_import_info(project_id, repo_type, repo_url):
  """Updates ProjectImportInfo if needed."""
  info = ProjectImportInfo.get_by_id(project_id)
  if info and info.repo_type == repo_type and info.repo_url == repo_url:
    return
  if info:
    values = (
      ('repo_url', repo_url, info.repo_url),
      ('repo_type', repo_type, info.repo_type),
    )
    logging.warning('Changing project %s repo info:\n',
        project_id,
        '\n'.join([
          '%s: %s -> %s' % (attr, old_value, new_value)
          for attr, old_value, new_value in values
          if old_value != new_value
        ]))
  ProjectImportInfo(id=project_id, repo_type=repo_type, repo_url=repo_url).put()


def get_projects():
  """Returns a list of projects stored in services/luci-config:projects.cfg.

  Never returns None. Cached.
  """
  cfg = storage.get_self_config(
      common.PROJECT_REGISTRY_FILENAME, service_config_pb2.ProjectsCfg)
  return cfg.projects or []


def project_exists(project_id):
  # TODO(nodir): optimize
  return any(p.id == project_id for p in get_projects())


def get_repo(project_id):
  """Returns tuple (repo_type, repo_url) for a project."""
  info = ProjectImportInfo.get_by_id(project_id)
  if info:
    return info.repo_type, info.repo_url
  return None, None


def get_metadata(project_id):
  """Returns project metadata stored in project.cfg.

  Never returns None.
  """
  return _get_project_config(
      project_id, common.PROJECT_METADATA_FILENAME,
      project_config_pb2.ProjectCfg)


def get_refs(project_id):
  """Returns list of project refs stored in refs.cfg.

  Never returns None.
  """
  cfg = _get_project_config(
      project_id, common.REFS_FILENAME, project_config_pb2.RefsCfg)
  # TODO(nodirt): implement globs.
  if not cfg.refs and not project_exists(project_id):
    return None
  return cfg.refs or DEFAULT_REF_CFG.refs


def _get_project_config(project_id, path, message_factory):
  assert project_id
  return storage.get_latest_as_message(
      'projects/%s' % project_id, path, message_factory)
