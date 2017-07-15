#!/usr/bin/python
# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Sample Machine Provider agent."""

import argparse
import datetime
import httplib2
import json
import logging
import os
import subprocess
import sys
import time
import traceback
import urlparse


THIS_DIR = os.path.dirname(__file__)

LOG_DIR = os.path.join(THIS_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, '%s.log' % os.path.basename(__file__))

METADATA_BASE_URL = 'http://metadata.google.internal/computeMetadata/v1'


class Error(Exception):
  pass


class RequestError(Error):
  def __init__(self, response, content):
    self.response = response
    self.content = content


# 404
class NotFoundError(RequestError):
  pass


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


class Agent(object):
  """Base class for Machine Provider agent."""

  def __init__(self, user):
    """Initializes a new instance of the Agent.

    Args:
      user: The username of the user the agent should run for.
    """
    self.user = user

  def configure_logging(self):
    """Sets up the log file."""
    class TimeFormatter(logging.Formatter):
      def formatTime(self, record, datefmt=None):
        datefmt = datefmt or '%Y-%m-%d %H:%M:%S'
        return time.strftime(datefmt, time.localtime(record.created))

    class SeverityFilter(logging.Filter):
      def filter(self, record):
        record.severity = record.levelname[0]
        return True

    path = os.path.join(self.LOGS_DIR, 'agent.%s.log' % time.time())
    if not os.path.exists(self.LOGS_DIR):
      os.makedirs(self.LOGS_DIR)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log = logging.FileHandler(path)
    log.addFilter(SeverityFilter())
    log.setFormatter(TimeFormatter('%(asctime)s %(severity)s: %(message)s'))
    logger.addHandler(log)

    # Log all uncaught exceptions.
    def log_exception(kind, value, trace):
      logging.error(''.join(traceback.format_exception(kind, value, trace)))
    sys.excepthook = log_exception

  def start(self):
    """Starts the Machine Provider agent."""
    raise NotImplementedError

  def stop(self):
    """Stops the Machine Provider agent."""
    raise NotImplementedError

  def chown(self, user, path):
    """Change ownership of a path.

    Args:
      user: The user which should own the path.
      path: The path to change ownership of.
    """
    raise NotImplementedError

  def reboot(self):
    """Reboots the machine."""
    logging.info('Rebooting...')
    while True:
      subprocess.check_call(self.REBOOT_CMD)
      time.sleep(60)
      logging.info('Waiting to reboot...')

  def install(self):
    """Installs the Machine Provider agent and configures autostart.

    Args:
      user: The username of the user to install the agent for.
    """
    with open(self.AGENT_AUTOSTART_TEMPLATE) as f:
      config = f.read() % {
          'agent': os.path.abspath(__file__),
          'user': self.user,
      }

    path = self.AGENT_AUTOSTART_PATH % {'user': self.user}
    if os.path.exists(path):
      with open(path) as f:
        if config == f.read():
          logging.info('Already installed: %s', path)
          return
      logging.info('Reinstalling: %s', path)
      self.stop()
    else:
      logging.info('Installing: %s', path)
    with open(path, 'w') as f:
      f.write(config)
    self.start()

  def download_swarming_bot_code(self, service_account, swarming_server, path):
    """Downloads the Swarming bot code.

    Args:
      service_account: Service account to authorize requests to Swarming with.
      swarming_server: URL of the Swarming server to download bot code from.
      path: Path to download the bot code to.
    """
    if os.path.exists(path):
      logging.info('Already installed: %s', path)
      return

    logging.info('Installing: %s', path)
    http = AuthorizedHTTPRequest(service_account=service_account)
    _, bot_code = http.request(
      urlparse.urljoin(swarming_server, 'bot_code'), method='GET')
    # Open for write as a binary file, otherwise Python may convert
    # end-of-line \n characters to the Windows-specific \r\n.
    with open(path, 'wb') as f:
      f.write(bot_code)
    self.chown(self.user, path)

  def connect_to_swarming(self, service_account, swarming_server):
    """Connects to the given Swarming server. Configures auto-connect on reboot.

    Args:
      service_account: Service account to authorize requests to Swarming with.
      swarming_server: URL of the Swarming server to connect to.

    Returns:
      True if successful, False otherwise.
    """
    if not os.path.exists(self.SWARMING_BOT_DIR):
      os.makedirs(self.SWARMING_BOT_DIR)
    self.chown(self.user, self.SWARMING_BOT_DIR)

    path = os.path.join(self.SWARMING_BOT_DIR, 'swarming_bot.zip')
    self.download_swarming_bot_code(service_account, swarming_server, path)

    with open(self.SWARMING_AUTOSTART_TEMPLATE) as f:
      config = f.read() % {
          'user': self.user,
          'zip': path,
      }

    path = self.SWARMING_AUTOSTART_PATH % {'user': self.user}
    if os.path.exists(path):
      logging.info('Reinstalling: %s', path)
      os.remove(path)
    else:
      logging.info('Installing: %s', path)
    with open(path, 'w') as f:
      f.write(config)

  def poll(self):
    """Polls Machine Provider for instructions."""
    # Metadata tells us which Machine Provider instance to talk to.
    hostname = get_metadata('instance/name')
    server = get_metadata('instance/attributes/machine_provider_server')
    service_account = get_metadata(
        'instance/attributes/machine_service_account')
    mp = MachineProvider(server=server, service_account=service_account)

    while True:
      logging.info('Polling for instructions')
      try:
        response = mp.poll(hostname, 'GCE')
        if response.get('instruction') and response.get('state') != 'EXECUTED':
          logging.info(
              'Received new instruction: %s', json.dumps(response, indent=2))
          if response.get('instruction', {}).get('swarming_server'):
            self.connect_to_swarming(
                service_account, response['instruction']['swarming_server'])
            mp.ack(hostname, 'GCE')
            self.reboot()
      except NotFoundError:
        logging.warning('Invalid hostname for GCE backend, or unauthorized')
      time.sleep(60)


class SystemdAgent(Agent):
  """Machine Provider agent for systemd-based Linux distributions.

  The agent is installed for root.
  """

  AGENT_AUTOSTART_TEMPLATE = os.path.join(
      THIS_DIR, 'machine-provider-agent.service.tmpl')
  AGENT_AUTOSTART_PATH = '/etc/systemd/system/machine-provider-agent.service'
  LOGS_DIR = '/var/log/messages/machine-provider-agent'
  REBOOT_CMD = ('/sbin/shutdown', '-r', 'now')
  SWARMING_AUTOSTART_TEMPLATE = os.path.join(
      THIS_DIR, 'swarming-start-bot.service.tmpl')
  SWARMING_AUTOSTART_PATH = '/etc/systemd/system/swarming-start-bot.service'
  SWARMING_BOT_DIR = '/b/s'

  def start(self):
    """Starts the Machine Provider agent."""
    subprocess.check_call(['systemctl', 'daemon-reload'])
    subprocess.check_call(['systemctl', 'enable', 'machine-provider-agent'])
    subprocess.check_call(['systemctl', 'start', 'machine-provider-agent'])

  def stop(self):
    """Stops the Machine Provider agent."""
    subprocess.check_call(['systemctl', 'stop', 'machine-provider-agent'])

  def chown(self, user, path):
    """Change ownership of a path.

    Args:
      user: The user which should own the path.
      path: The path to change ownership of.
    """
    import pwd
    user = pwd.getpwnam(user)
    os.chown(path, user.pw_uid, user.pw_gid)

  def connect_to_swarming(self, service_account, swarming_server):
    super(Agent, self).connect_to_swarming(service_account, swarming_server)
    subprocess.check_call(['systemctl', 'enable', 'swarming-start-bot'])


class UpstartAgent(Agent):
  """Machine Provider agent for Upstart.

  The agent is installed for root.
  """

  AGENT_AUTOSTART_TEMPLATE = os.path.join(
      THIS_DIR, 'machine-provider-agent.conf.tmpl')
  AGENT_AUTOSTART_PATH = '/etc/init/machine-provider-agent.conf'
  LOGS_DIR = '/var/log/messages/machine-provider-agent'
  REBOOT_CMD = ('/sbin/shutdown', '-r', 'now')
  SWARMING_AUTOSTART_TEMPLATE = os.path.join(
      THIS_DIR, 'swarming-start-bot.conf.tmpl')
  SWARMING_AUTOSTART_PATH = '/etc/init/swarming-start-bot.conf'
  SWARMING_BOT_DIR = '/b/s'

  def start(self):
    """Starts the Machine Provider agent."""
    subprocess.check_call(['initctl', 'reload-configuration'])
    subprocess.check_call(['start', 'machine-provider-agent'])

  def stop(self):
    """Stops the Machine Provider agent."""
    subprocess.check_call(['stop', 'machine-provider-agent'])

  def chown(self, user, path):
    """Change ownership of a path.

    Args:
      user: The user which should own the path.
      path: The path to change ownership of.
    """
    import pwd
    user = pwd.getpwnam(user)
    os.chown(path, user.pw_uid, user.pw_gid)


class WindowsAgent(Agent):
  """Machine Provider agent for Windows.

  The agent is installed for self.user.
  """

  AGENT_AUTOSTART_TEMPLATE = os.path.join(
      THIS_DIR, 'machine-provider-agent.bat.tmpl')
  AGENT_AUTOSTART_PATH = (
      'C:\\Users\\%(user)s\\Start Menu\\Programs\\Startup\\'
      'machine-provider-agent.bat'
  )
  LOGS_DIR = 'C:\\logs'
  REBOOT_CMD = ('shutdown', '/f', '/r', '/t', '0')
  SWARMING_AUTOSTART_TEMPLATE = os.path.join(
      THIS_DIR, 'swarming-start-bot.bat.tmpl')
  SWARMING_AUTOSTART_PATH = (
      'C:\\Users\\%(user)s\\Start Menu\\Programs\\Startup\\'
      'swarming-start-bot.bat'
  )
  SWARMING_BOT_DIR = 'C:\\b\\s'

  def start(self):
    """Starts the Machine Provider agent."""
    path = self.AGENT_AUTOSTART_PATH % {'user': self.user}
    subprocess.Popen(
        [path],
        # Prevents handles from being inherited on Windows.
        close_fds=True,
        # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863.aspx
        # CREATE_NEW_PROCESS_GROUP: 0x200
        # DETACHED_PROCESS:         0x008
        creationflags=0x200 | 0x8,
    )

  def stop(self):
    """Stops the Machine Provider agent."""
    # TODO(smut): Stop the agent.
    pass

  def chown(self, user, path):
    """Change ownership of a path.

    Args:
      user: The user which should own the path.
      path: The path to change ownership of.
    """
    # TODO(smut): Determine if this is necessary on our Windows VMs.
    pass


def main():
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

  if sys.platform == 'linux2':
    # TODO(smut): Stop assuming chrome-bot.
    if subprocess.call(['which', 'systemctl']) == 0:
      agent = SystemdAgent(args.user or 'chrome-bot')
    elif 'upstart' in subprocess.check_output(['init', '--version']):
      agent = UpstartAgent(args.user or 'chrome-bot')
    else:
      # TODO(phosek): Shall we add support for LSB init?
      logging.error('Unsupported init system')
  elif sys.platform == 'win32':
    agent = WindowsAgent(args.user)

  agent.configure_logging()

  if args.install:
    return agent.install()

  return agent.poll()


if __name__ == '__main__':
  sys.exit(main())
