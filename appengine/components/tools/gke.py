#!/usr/bin/env python
# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Wrapper around GKE (Google Container Engine) SDK tools to simplify working
with container deployments.

Core to this is a services configuration YAML file. This lists all of the
GKE services that get deployed. A file declares clusters, each of which
is a single GKE pod deployment. An example YAML follows:

project: my-project
clusters:
  my-service:
    path: relpath/to/Dockerfile/directory
    name: gke-cluster-name
    zone: cluster-zone
    replicas: number-of-replicas
    collect_gopath: _gopath

Breaking this down, this declares that the YAML applies to the Google cloud
project called "my-project". It defines one microservice, "my-service". The
service is rooted at the specified relative path and uses the GKE cluster
named "gke-cluster-name" in zone "cluster-zone".

When deployed, the cluster will request "number-of-replicas" replicas.

Finally, we collect GOPATH in a subdirectory "_gopath" underneath of "path".
This means that all source folders in GOPATH with ".go" files in them are
copied (hardlink, so fast) into that directory. This is necessary so Docker
can deploy against the local checkout of those files instead of using "go get"
to fetch them from HEAD.
"""

__version__ = '1.0'

import argparse
import collections
import logging
import os
import shutil
import subprocess
import sys
import time

SCRIPT_PATH = os.path.abspath(__file__)
ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, '..', 'third_party_local'))

# Load and import the AppEngine SDK environment.
from tools import calculate_version
from tool_support import gae_sdk_utils
gae_sdk_utils.setup_gae_env()

import yaml

# Enable "yaml" to dump ordered dict.
def dict_representer(dumper, data):
  return dumper.represent_dict((k, v) for k, v in data.iteritems()
                               if v is not None)
yaml.Dumper.add_representer(collections.OrderedDict, dict_representer)


def run_command(cmd, cwd=None):
  logging.info('Running command: %s', cmd)
  return subprocess.call(
      cmd,
      stdin=sys.stdin,
      stdout=sys.stdout,
      stderr=sys.stderr,
      cwd=cwd)


def check_output(cmd, cwd=None):
  logging.info('Running command (cwd=%r): %s', cwd, cmd)
  return subprocess.check_output(cmd, cwd=cwd)


def collect_gopath(base, name, gopath):
  dst = os.path.join(base, name)
  logging.debug('Collecting GOPATH from %r into %r.', gopath, dst)
  if os.path.exists(dst):
    shutil.rmtree(dst)
  os.makedirs(dst)

  # Our goal is to recursively copy ONLY Go directories. We detect these as
  # directories with at least one ".go" file in them. Any other directory, we
  # ignore.
  #
  # We'll also hardlink the actual files so we don't waste space.
  #
  # Unfortunately, there's not really a good OS or Python facility to do this.
  # It's simple enough, though, so we'll just do it ourselves.
  #
  # Also, in true emulation of GOPATH, we want any earlier Go directories to
  # override later ones. We'll track Go directories (relative) and not copy
  # other directories if they happen to have duplicates.
  seen = set()
  for p in gopath.split(os.pathsep):
    go_src = os.path.join(p, 'src')
    if not os.path.isdir(go_src):
      continue

    for root, dirs, files in os.walk(go_src):
      # Do not recurse into our destination directory, which is likely itself
      # on GOPATH.
      if os.path.samefile(root, dst):
        del(dirs[:])
        continue
      if not any(f.endswith('.go') for f in files):
        continue
      rel = os.path.relpath(root, go_src)
      if rel in seen:
        continue
      seen.add(rel)

      rel_dst = os.path.join(dst, 'src', rel)
      if not os.path.isdir(rel_dst):
        os.makedirs(rel_dst)
      logging.debug('Copying Go source %r => %r', root, rel_dst)
      for f in files:
        os.link(os.path.join(root, f), os.path.join(rel_dst, f))


class Configuration(object):
  """Cluster is the configuration for a single GKE application.
  """

  # Config is the configuration schema.
  Config = collections.namedtuple('Config', (
      'project', 'clusters',
  ))
  Cluster = collections.namedtuple('Cluster', (
      'name', 'path', 'zone', 'replicas', 'collect_gopath'))

  def __init__(self, config):
    self.config = config

  def __getattr__(self, key):
    return getattr(self.config, key)

  @staticmethod
  def load_with_defaults(typ, d):
    inst = typ(**{v: None for v in typ._fields})
    inst = inst._replace(**d)
    return inst

  @classmethod
  def write_template(cls, path):
    # Generate a template config and write it.
    cfg = cls.Config(
      project='project',
      clusters={
        'cluster-key': cls.Cluster(
          name='cluster-name',
          zone='cluster-zone',
          path='relpath/to/docker/root',
          replicas=1,
          collect_gopath=None,
        )._asdict(),
      },
    )
    with open(path, 'w') as fd:
      yaml.dump(cfg._asdict(), fd, default_flow_style=False)

  @classmethod
  def load(cls, path):
    if not os.path.isfile(path):
      cls.write_template(path)
      raise ValueError('Missing configuration path. A template was generated '
                       'at: %r' % (path,))

    with open(path, 'r') as fd:
      d = yaml.load(fd)

    # Load the JSON into our namedtuple schema.
    cfg = cls.load_with_defaults(cls.Config, d)
    cfg = cfg._replace(clusters={k: cls.load_with_defaults(cls.Cluster, v)
                                 for k, v in cfg.clusters.iteritems()})

    return cls(cfg)


class Application(object):
  """Cluster is the configuration for a single GKE application.

  This includes the application's name, project, deployment parameters.

  An application consists of:
  - A path to its root Dockerfile.
  - Deployment cluster parameters (project, zone, etc.), loaded from JSON.
  """

  def __init__(self, config_dir, config, cluster_key):
    self.timestamp = int(time.time())
    self.config_dir = config_dir
    self.cfg = config
    self.cluster_key = cluster_key

  @property
  def project(self):
    return self.cfg.project
  @property
  def cluster(self):
    return self.cfg.clusters[self.cluster_key]
  @property
  def name(self):
    return self.cluster.name
  @property
  def zone(self):
    return self.cluster.zone
  @property
  def app_dir(self):
    return os.path.join(self.config_dir, self.cluster.path)

  def ensure_dockerfile(self):
    dockerfile_path = os.path.join(self.app_dir, 'Dockerfile')
    if not os.path.isfile(dockerfile_path):
      raise ValueError('No Dockerfile at %r.' % (dockerfile_path,))
    return dockerfile_path

  @property
  def kubectl_context(self):
    """Returns the name of the "kubectl" context for this Application.

    This name is a "gcloud" implementation detail, and may change in the future.
    Knowing the name, we can check for credential provisioning locally, reducing
    the common case overhead of individual "kubectl" invocations.
    """
    return 'gke_%s_%s_%s' % (self.project, self.zone, self.name)

  def calculate_version(self, tag=None):
    return calculate_version.calculate_version(self.app_dir, tag)

  def image_tag(self, version):
    # We add a timestamp b/c otherwise Docker can't distinguish two images with
    # the same tag, and won't update.
    return 'gcr.io/%s/%s:%s-%d' % (
        self.project, self.name, version, self.timestamp)

  def write_deployment_yaml(self, image_tag, version):
    v = collections.OrderedDict()
    v['apiVersion'] = 'apps/v1beta1'
    v['kind'] = 'Deployment'
    v['metadata'] = collections.OrderedDict((
        ('name', self.cluster_key),
    ))

    v['spec'] = collections.OrderedDict()
    v['spec']['replicas'] = self.cluster.replicas or 0

    v['spec']['template'] = collections.OrderedDict()
    v['spec']['template']['metadata'] = collections.OrderedDict()
    v['spec']['template']['metadata']['labels'] = collections.OrderedDict((
        ('luci/project', self.project),
        ('luci/cluster', self.cluster_key),
    ))
    v['spec']['template']['metadata']['annotations'] = collections.OrderedDict((
        ('luci.managedBy', 'luci-gke-py'),
        ('luci-gke-py/version', version),
    ))

    v['spec']['template']['spec'] = collections.OrderedDict()
    v['spec']['template']['spec']['containers'] = [
        collections.OrderedDict((
            ('name', self.cluster_key),
            ('image', image_tag),
        )),
    ]

    deploy_yaml_path = os.path.join(self.config_dir, 'deployment.yaml')
    with open(deploy_yaml_path, 'w') as fd:
      yaml.dump(v, fd, default_flow_style=False)
    return deploy_yaml_path


class Kubectl(object):
  """Wrapper around the "kubectl" tool.
  """

  def __init__(self, app, needs_refresh):
    self.app = app
    self._needs_refresh = needs_refresh

  @property
  def executable(self):
    return 'kubectl'

  def run(self, cmd, **kwargs):
    args = [
        self.executable,
        '--context', self.ensure_app_context(),
    ]
    args.extend(cmd)
    return run_command(args, **kwargs)

  def check_gcloud(self, cmd, **kwargs):
    args = [
        'gcloud',
        '--project', self.app.project,
    ]
    args.extend(cmd)
    return check_output(args, **kwargs)

  def check_output(self, *cmd, **kwargs):
    context = kwargs.pop('context', self.app.kubectl_context)
    args = [
        self.executable,
        '--context', context,
    ]
    args.extend(cmd)
    return check_output(args, **kwargs)

  def _has_app_context(self):
    # If this command returns non-zero and has non-empty output, we know that
    # the context is available.
    stdout = self.check_output(
        'config',
        'view',
        '--output', 'jsonpath=\'{.users[?(@.name == "%s")].name}\'' % (
            self.app.kubectl_context,),
        context='',
    )
    stdout = stdout.strip()
    return stdout and stdout != "''"

  def ensure_app_context(self):
    """Sets the current "kubectl" context to point to the current application.

    Kubectl can contain multiple context configurations. We want to explicitly
    specify the context each time we execute a command.

    Worst-case, we need to use "gcloud" to provision the context, which includes
    round-trips from remote services. Best-case, we're already provisioned with
    the context and can just use it.

    Returns (str): The name of the Kubernetes context, suitable for passing
      to the "--context" command-line parameter.
    """
    if self._needs_refresh or not self._has_app_context():
      self.check_gcloud([
          'container',
          'clusters',
          'get-credentials',
          self.app.name,
          '--zone', self.app.zone,
      ])
      self._needs_refresh = False
      if not self._has_app_context():
        raise Exception('Kubernetes context missing after provisioning.')
    return self.app.kubectl_context


def subcommand_kubectl(args, kctl):
  """Runs a Kubernetes command in the context of the configured Application.
  """
  cmd = args.args
  if cmd and cmd[0] == '--':
    cmd = cmd[1:]
  return kctl.run(cmd)


def subcommand_deploy(args, kctl):
  """Deploys a Kubernetes instance."""

  # Determine our version and Docker image tag.
  version = kctl.app.calculate_version(tag=args.tag)
  image_tag = kctl.app.image_tag(version)

  # Generate our deployment YAML, in the same path as our config.
  deploy_yaml = kctl.app.write_deployment_yaml(image_tag, version)

  # If we need to collect GOPATH, do it.
  if kctl.app.cluster.collect_gopath:
    collect_gopath(
        kctl.app.app_dir,
        kctl.app.cluster.collect_gopath,
        os.environ.get('GOPATH', ''))

  # Build our Docker image.
  kctl.check_gcloud(
      ['docker', '--', 'build', '-t', image_tag, '.'],
      cwd=kctl.app.app_dir)

  # Push the Docker image to the project's registry.
  kctl.check_gcloud(
      ['docker', '--', 'push', image_tag])

  # Deploy to Kubernetes.
  kctl.run(['apply', '-f', deploy_yaml])


def main(args):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-v', '--verbose',
      action='count',
      default=0,
      help='Increase logging verbosity. Can be specified multiple times.')
  parser.add_argument(
      '-r', '--force-refresh',
      action='store_true',
      help='Forcefully refresh GKE authentication.')
  parser.add_argument(
      '-C', '--config', required=True,
      help='Path to the cluster configuration JSON file.')
  subparsers = parser.add_subparsers()

  def add_cluster_key_arg(subparser):
    subparser.add_argument(
        '-K', '--cluster-key', required=True,
        help='Key of the cluster within the config to work with.')

  # Subcommand: kubectl
  subparser = subparsers.add_parser('kubectl',
      help='Direct invocation of "kubectl" command using target context.')
  subparser.add_argument('args', nargs=argparse.REMAINDER,
      help='Arguments to pass to the "kubectl" invocation.')
  add_cluster_key_arg(subparser)
  subparser.set_defaults(func=subcommand_kubectl)

  # Subcommand: deploy
  subparser = subparsers.add_parser('deploy',
      help='Build and deploy a new instance to a Kubernetes cluster.')
  subparser.add_argument('-t', '--tag',
      help='Optional tag to add to the version.')
  add_cluster_key_arg(subparser)
  subparser.set_defaults(func=subcommand_deploy)

  args = parser.parse_args()

  if args.verbose == 0:
    logging.getLogger().setLevel(logging.WARNING)
  elif args.verbose == 1:
    logging.getLogger().setLevel(logging.INFO)
  else:
    logging.getLogger().setLevel(logging.DEBUG)

  config_path = os.path.abspath(args.config)
  config = Configuration.load(config_path)
  if args.cluster_key not in config.clusters:
    raise ValueError(
        'A cluster key is required (--cluster-key), one of: %s' % (
          ', '.join(sorted(config.clusters.keys())),)
    )

  app = Application(
      os.path.dirname(config_path),
      config,
      args.cluster_key)
  kctl = Kubectl(app, args.force_refresh)
  return args.func(args, kctl)


if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  sys.exit(main(sys.argv[1:]))
