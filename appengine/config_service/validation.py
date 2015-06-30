# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

import base64
import logging
import posixpath
import re
import urlparse

from google.appengine.ext import ndb

from components import config
from components import gitiles
from components import net
from components.config import validation

from proto import project_config_pb2
from proto import service_config_pb2
import common
import storage


def validate_config_set(config_set, ctx=None):
  ctx = ctx or validation.Context.raise_on_error()
  if not any(r.match(config_set) for r in config.ALL_CONFIG_SET_RGX):
    ctx.error('invalid config set: %s', config_set)


def validate_path(path, ctx=None):
  ctx = ctx or validation.Context.raise_on_error(prefix='Invalid path: ')
  if not path:
    ctx.error('not specified')
    return
  if posixpath.isabs(path):
    ctx.error('must not be absolute: %s', path)
  if any(p in ('.', '..') for p in path.split(posixpath.sep)):
    ctx.error('must not contain ".." or "." components: %s', path)


def validate_url(url, ctx):
  if not url:
    ctx.error('not specified')
    return
  parsed = urlparse.urlparse(url)
  if not parsed.netloc:
    ctx.error('hostname not specified')
  if parsed.scheme != 'https':
    ctx.error('scheme must be "https"')


def validate_pattern(pattern, literal_validator, ctx):
  if ':' not in pattern:
    literal_validator(pattern, ctx)
    return

  pattern_type, pattern_text = pattern.split(':', 2)
  if pattern_type != 'regex':
    ctx.error('unknown pattern type: %s', pattern_type)
    return
  try:
    re.compile(pattern_text)
  except re.error as ex:
    ctx.error('invalid regular expression "%s": %s', pattern_text, ex)


@validation.self_rule(
    common.VALIDATION_FILENAME, service_config_pb2.ValidationCfg)
def validate_validation_cfg(cfg, ctx):
  for i, rule in enumerate(cfg.rules):
    with ctx.prefix('Rule #%d: ', i + 1):
      with ctx.prefix('config_set: '):
        validate_pattern(rule.config_set, validate_config_set, ctx)
      with ctx.prefix('path: '):
        validate_pattern(rule.path, validate_path, ctx)
      with ctx.prefix('url: '):
        validate_url(rule.url, ctx)


@validation.self_rule(
    common.PROJECT_REGISTRY_FILENAME, service_config_pb2.ProjectsCfg)
def validate_project_registry(cfg, ctx):
  project_ids = set()
  unsorted_id = None
  for i, project in enumerate(cfg.projects):
    with ctx.prefix('Project %s: ', project.id or ('#%d' % (i + 1))):
      if not project.id:
        ctx.error('id is not specified')
      else:
        if project.id in project_ids:
          ctx.error('id is not unique')
        else:
          project_ids.add(project.id)
        if not unsorted_id and i > 0:
          if cfg.projects[i - 1].id and project.id < cfg.projects[i - 1].id:
            unsorted_id = project.id

      if project.config_storage_type == service_config_pb2.Project.UNSET:
        ctx.error('config_storage_type is not set')
      else:
        assert project.config_storage_type == service_config_pb2.Project.GITILES
        try:
          gitiles.Location.parse(project.config_location)
        except ValueError as ex:
          ctx.error('config_location: %s', ex)

  if unsorted_id:
    ctx.warning(
        'Project list is not sorted by id. First offending id: %s',
        unsorted_id)


@validation.self_rule(common.ACL_FILENAME, service_config_pb2.AclCfg)
def validate_acl_cfg(_cfg, _ctx):
  # A valid protobuf message is enough.
  pass

@validation.self_rule(common.IMPORT_FILENAME, service_config_pb2.ImportCfg)
def validate_import_cfg(_cfg, _ctx):
  # A valid protobuf message is enough.
  pass


@validation.self_rule(common.SCHEMAS_FILENAME, service_config_pb2.SchemasCfg)
def validate_schemas(cfg, ctx):
  names = set()
  for i, schema in enumerate(cfg.schemas):
    with ctx.prefix('Schema %s: ', schema.name or '#%d' % (i + 1)):
      if not schema.name:
        ctx.error('name is not specified')
      elif ':' not in schema.name:
        ctx.error('name must contain ":"')
      else:
        if schema.name in names:
          ctx.error('duplicate schema name')
        else:
          names.add(schema.name)

        config_set, path = schema.name.split(':', 2)
        if (not config.SERVICE_CONFIG_SET_RGX.match(config_set) and
            config_set not in ('projects', 'projects/refs')):
          ctx.error(
              'left side of ":" must be a service config set, "projects" or '
              '"projects/refs"')
        validate_path(path, ctx)
      with ctx.prefix('url: '):
        validate_url(schema.url, ctx)


@validation.project_config_rule(
    common.PROJECT_METADATA_FILENAME, project_config_pb2.ProjectCfg)
def validate_project_metadata(cfg, ctx):
  if not cfg.name:
    ctx.error('name is not specified')


@validation.project_config_rule(
    common.REFS_FILENAME, project_config_pb2.RefsCfg)
def validate_refs_cfg(cfg, ctx):
  refs = set()
  for i, ref in enumerate(cfg.refs):
    with ctx.prefix('Ref #%d: ', i + 1):
      if not ref.name:
        ctx.error('name is not specified')
      elif not ref.name.startswith('refs/'):
        ctx.error('name does not start with "refs/": %s', ref.name)
      elif ref.name in refs:
        ctx.error('duplicate ref: %s', ref.name)
      else:
        refs.add(ref.name)
      if ref.config_path:
        validate_path(ref.config_path, ctx)


@ndb.tasklet
def _endpoint_validate_async(url, config_set, path, content, ctx):
  """Validates a config with an external service."""
  res = None

  def report_error(text):
    text = (
        'Error during external validation: %s\n'
        'url: %s\n'
        'config_set: %s\n'
        'path: %s\n'
        'response: %r') % (text, url, config_set, path, res)
    logging.error(text)
    ctx.critical(text)

  try:
    req = {
      'config_set': config_set,
      'path': path,
      'content': base64.b64encode(content),
    }
    res = yield net.json_request_async(
        url, method='POST', payload=req,
        scope='https://www.googleapis.com/auth/userinfo.email')
  except net.Error as ex:
    report_error('Net error: %s' % ex)
    return

  try:
    for msg in res.get('messages', []):
      if not isinstance(msg, dict):
        report_error('invalid response: message is not a dict: %r' % msg)
        continue
      severity = msg.get('severity') or 'INFO'
      if (severity not in
          service_config_pb2.ValidationResponseMessage.Severity.keys()):
        report_error(
            'invalid response: unexpected message severity: %s' % severity)
        continue
      # It is safe because we've validated |severity|.
      func = getattr(ctx, severity.lower())
      func(msg.get('text') or '')
  except Exception as ex:
    report_error(ex)


@ndb.tasklet
def validate_config_async(config_set, path, content, ctx=None):
  """Validates a config against built-in and external validators.

  External validators are defined in validation.cfg,
  see proto/service_config.proto.

  Returns:
    components.config.validation_context.Result.
  """
  ctx = ctx or validation.Context()

  # Check the config against built-in validators,
  # defined using validation.self_rule.
  validation.validate(config_set, path, content, ctx=ctx)

  validation_cfg = storage.get_self_config(
      common.VALIDATION_FILENAME, service_config_pb2.ValidationCfg)
  # Be paranoid, check yourself.
  validate_validation_cfg(validation_cfg, validation.Context.raise_on_error())

  futures = []
  for rule in validation_cfg.rules:
    if (_pattern_match(rule.config_set, config_set) and
        _pattern_match(rule.path, path)):
      futures.append(
          _endpoint_validate_async(rule.url, config_set, path, content, ctx))
  yield futures
  raise ndb.Return(ctx.result())


def validate_config(*args, **kwargs):
  """Blocking version of validate_async."""
  return validate_config_async(*args, **kwargs).get_result()


def _pattern_match(pattern, value):
  # Assume pattern is valid.
  if ':' not in pattern:
    return pattern == value
  else:
    kind, pattern = pattern.split(':', 2)
    assert kind == 'regex'
    return bool(re.match('^%s$' % pattern, value))
