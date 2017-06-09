#!/usr/bin/python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Machine Provider agent for GCE Linux instances."""

import argparse
import datetime
import httplib2
import json
import logging
import logging.handlers
import os
import pwd
import subprocess
import sys
import time
import traceback
import urlparse


THIS_DIR = os.path.dirname(__file__)

LOG_DIR = os.path.join(THIS_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, '%s.log' % os.path.basename(__file__))

AGENT_UPSTART_CONFIG_DEST = '/etc/init/machine-provider-agent.conf'
AGENT_UPSTART_CONFIG_TMPL = os.path.join(
    THIS_DIR, 'machine-provider-agent.conf.tmpl')
AGENT_UPSTART_JOB = 'machine-provider-agent'
CHROME_BOT = 'chrome-bot' # TODO(smut): Remove after configs pass --user.
METADATA_BASE_URL = 'http://metadata/computeMetadata/v1'
SWARMING_BOT_DIR = '/b/s'
SWARMING_BOT_ZIP = os.path.join(SWARMING_BOT_DIR, 'swarming_bot.zip')
SWARMING_UPSTART_CONFIG_DEST = '/etc/init/swarming-start-bot.conf'
SWARMING_UPSTART_CONFIG_TMPL = os.path.join(
    THIS_DIR, 'swarming-start-bot.conf.tmpl')


class Error(Exception):
  pass


class RequestError(Error):
  def __init__(self, response, content):
    self.response = response
    self.content = content


# 404
class NotFoundError(RequestError):
  pass


def configure_logging():
  """Sets up the log file."""
  class TimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
      datefmt = datefmt or '%Y-%m-%d %H:%M:%S'
      return time.strftime(datefmt, time.localtime(record.created))

  class SeverityFilter(logging.Filter):
    def filter(self, record):
      record.severity = record.levelname[0]
      return True

  if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

  logger = logging.getLogger()
  logger.setLevel(logging.DEBUG)

  log_file = logging.handlers.RotatingFileHandler(LOG_FILE, backupCount=100)
  log_file.addFilter(SeverityFilter())
  log_file.setFormatter(TimeFormatter('%(asctime)s %(severity)s: %(message)s'))
  logger.addHandler(log_file)

  # Log all uncaught exceptions.
  def log_exception(exception_type, value, stack_trace):
    logging.error(
        ''.join(traceback.format_exception(exception_type, value, stack_trace)),
    )
  sys.excepthook = log_exception

  # Rotate log files once on startup to get per-execution log files.
  if os.path.exists(LOG_FILE):
    log_file.doRollover()


def get_metadata(key=''):
  """Retrieves the specified metadata from the metadata server.

  Args:
    key: The key whose value should be returned from the metadata server.

  Returns:
    The value associated with the metadata key.

  Raises:
    httplib2.ServerNotFoundError: If the metadata server cannot be found.
      Probably indicates that this script is not running on a GCE instance.
    NotFoundError: If the metadata key could not be found.
  """
  response, content = httplib2.Http().request(
      '%s/%s' % (METADATA_BASE_URL, key),
      headers={'Metadata-Flavor': 'Google'},
      method='GET',
  )
  if response['status'] == '404':
    raise NotFoundError(response, content)
  return content


class AuthorizedHTTPRequest(httplib2.Http):
  """Subclass for HTTP requests authorized with service account credentials."""

  def __init__(self, service_account='default'):
    super(AuthorizedHTTPRequest, self).__init__()
    self._expiration_time = datetime.datetime.now()
    self._service_account = service_account
    self._token = None
    self.refresh_token()

  @property
  def service_account(self):
    return self._service_account

  @property
  def token(self):
    return self._token

  def refresh_token(self):
    """Fetches and caches the token from the metadata server."""
    token = json.loads(get_metadata(
        'instance/service-accounts/%s/token' % self.service_account,
    ))
    seconds = token['expires_in'] - 60
    self._expiration_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=seconds)
    )
    self._token = token['access_token']

  def request(self, *args, **kwargs):
    # Tokens fetched from the metadata server are valid for at least
    # 10 more minutes, so the token will still be valid for the request
    # (i.e. no risk of refreshing to a token that is about to expire).
    if self._expiration_time < datetime.datetime.now():
      self.refresh_token()
    kwargs['headers'] = kwargs.get('headers', {}).copy()
    kwargs['headers']['Authorization'] = 'Bearer %s' % self.token
    return super(AuthorizedHTTPRequest, self).request(*args, **kwargs)


class MachineProvider(object):
  """Class for interacting with Machine Provider."""

  def __init__(self, server, service_account='default'):
    """Initializes a new instance of the MachineProvider class.

    Args:
      server: URL of the Machine Provider server to talk to.
      service_account: Service account to authenticate requests with.
    """
    self._server = server
    self._http = AuthorizedHTTPRequest(service_account=service_account)

  def ack(self, hostname, backend):
    """Acknowledges the receipt and execution of an instruction.

    Args:
      hostname: Hostname of the machine whose instruction to acknowledge.
      backend: Backend the machine belongs to.

    Raises:
      NotFoundError: If there is no such instruction.
    """
    response, content = self._http.request(
        '%s/_ah/api/machine/v1/ack' % self._server,
        body=json.dumps({'backend': backend, 'hostname': hostname}),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    if response['status'] == '404':
      raise NotFoundError(response, content)
    return content

  def poll(self, hostname, backend):
    """Polls Machine Provider for instructions.

    Args:
      hostname: Hostname of the machine whose instructions to poll for.
      backend: Backend the machine belongs to.

    Returns:
      A JSON response from the Machine Provider.

    Raises:
      NotFoundError: If there is no such instruction.
    """
    response, content = self._http.request(
        '%s/_ah/api/machine/v1/poll' % self._server,
        body=json.dumps({'backend': backend, 'hostname': hostname}),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    if response['status'] == '404':
      raise NotFoundError(response, json.loads(content))
    return json.loads(content)


def connect_to_swarming(service_account, swarming_server, user):
  """Connects to the given Swarming server. Sets up auto-connect on reboot.

  Args:
    service_account: Service account to authenticate with to Swarming with.
    swarming_server: URL of the Swarming server to connect to.
    user: Username that Swarming bot process will run as.
  """
  with open(SWARMING_UPSTART_CONFIG_TMPL) as f:
    config = f.read().format(user=user)

  if os.path.exists(SWARMING_UPSTART_CONFIG_DEST):
    logging.info('Reinstalling: %s', SWARMING_UPSTART_CONFIG_DEST)
    os.remove(SWARMING_UPSTART_CONFIG_DEST)
  else:
    logging.info('Installing: %s', SWARMING_UPSTART_CONFIG_DEST)

  with open(SWARMING_UPSTART_CONFIG_DEST, 'w') as f:
    f.write(config)

  if not os.path.exists(SWARMING_BOT_DIR):
    os.mkdir(SWARMING_BOT_DIR)
  user = pwd.getpwnam(user)
  os.chown(SWARMING_BOT_DIR, user.pw_uid, user.pw_gid)

  if os.path.exists(SWARMING_BOT_ZIP):
    # Delete just the zip, not the whole directory so logs are kept.
    os.remove(SWARMING_BOT_ZIP)

  http = AuthorizedHTTPRequest(service_account=service_account)
  _, bot_code = http.request(
    urlparse.urljoin(swarming_server, 'bot_code'),
    method='GET',
  )
  with open(SWARMING_BOT_ZIP, 'w') as f:
    f.write(bot_code)
  os.chown(SWARMING_BOT_ZIP, user.pw_uid, user.pw_gid)


def poll(user):
  """Polls Machine Provider for instructions."""
  # Metadata tells us which Machine Provider instance to talk to.
  hostname = get_metadata('instance/name')
  server = get_metadata('instance/attributes/machine_provider_server')
  service_account = get_metadata('instance/attributes/machine_service_account')
  user = user or CHROME_BOT
  mp = MachineProvider(server=server, service_account=service_account)

  while True:
    logging.info('Polling for instructions')
    try:
      response = mp.poll(hostname, 'GCE')
      if response.get('instruction') and response.get('state') != 'EXECUTED':
        logging.info(
            'Received new instruction: %s', json.dumps(response, indent=2))
        if response.get('instruction', {}).get('swarming_server'):
          connect_to_swarming(
              service_account, response['instruction']['swarming_server'], user)
          mp.ack(hostname, 'GCE')
          subprocess.check_call(['/sbin/shutdown', '-r', 'now'])
    except NotFoundError:
      logging.warning('Invalid hostname for GCE backend, or unauthorized')
    time.sleep(60)


def install():
  """Installs Upstart config for the Machine Provider agent."""
  with open(AGENT_UPSTART_CONFIG_TMPL) as f:
    config = f.read().format(agent=os.path.abspath(__file__))

  if os.path.exists(AGENT_UPSTART_CONFIG_DEST):
    with open(AGENT_UPSTART_CONFIG_DEST) as f:
      existing_config = f.read()
    if config == existing_config:
      logging.info('Already installed: %s', AGENT_UPSTART_CONFIG_DEST)
      return
    else:
      logging.info('Reinstalling: %s', AGENT_UPSTART_CONFIG_DEST)
      os.remove(AGENT_UPSTART_CONFIG_DEST)
  else:
    logging.info('Installing: %s', AGENT_UPSTART_CONFIG_DEST)

  with open(AGENT_UPSTART_CONFIG_DEST, 'w') as f:
    f.write(config)
  subprocess.check_call(['initctl', 'reload-configuration'])
  subprocess.check_call(['start', AGENT_UPSTART_JOB])


def main():
  configure_logging()

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-i',
      '--install',
      action='store_true',
      help='Install Upstart config for the Machine Provider agent.',
  )
  parser.add_argument(
      '-u',
      '--user',
      help='User to set up Swarming for.',
  )
  args = parser.parse_args()

  if args.install:
    return install()

  return poll(args.user)


if __name__ == '__main__':
  sys.exit(main())
