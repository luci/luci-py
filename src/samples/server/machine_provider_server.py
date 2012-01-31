#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""This implements a machine provider server using a simple json file."""


import BaseHTTPServer
import cgi
try:
  import simplejson as json  # pylint: disable-msg=C6204
except ImportError:
  import json  # pylint: disable-msg=C6204
import logging  # pylint: disable-msg=C6204
import urlparse


class MachineProviderServer(BaseHTTPServer.BaseHTTPRequestHandler):
  """A server to be used by a Machine Provider based on a JSON file."""

  # We can override __init__ to provide more inputs since it's called by
  # BaseHTTPServer.HTTPServer, so we use public static members instead... Yuck!

  # The name of the file to store the machine list.
  machine_list_filename = 'machine_list'
  verbose = False

  def log_message(self, format_str, *args):
    if self.verbose:
      return BaseHTTPServer.BaseHTTPRequestHandler.log_message(self, format_str,
                                                               *args)

  def _LoadData(self):
    """Loads the data file identified by self.machine_list_filename.

    Raises:
      IOError as raised by open.
      ValueError if the file content isn't JSON.

    Returns:
      A decoded JSON dictionary with the file content.
    """
    machine_list_file = None
    try:
      machine_list_file = open(self.machine_list_filename, 'r')
      data = json.load(machine_list_file)
      machine_list_file.close()
      return data
    except (IOError, ValueError), e:
      logging.exception(e)
      if machine_list_file:
        machine_list_file.close()
      raise

  def _SaveData(self, data):
    """Saves the data in the file identified by self.machine_list_filename.

    Args:
      data: The dictionary to save as JSON.

    Raises:
      IOError as raised by open.
      ValueError if the data content isn't JSON.
    """
    machine_list_file = None
    try:
      machine_list_file = open(self.machine_list_filename, 'w')
      json.dump(data, machine_list_file)
      machine_list_file.close()
    except (IOError, ValueError), e:
      logging.exception(e)
      if machine_list_file:
        machine_list_file.close()
      raise

  def _MatchDimension(self, requested_dimensions, machine_dimensions):
    """Identify if the machine's dimensions match the requested dimensions.

    Note that the machine could have more than what is requested but it must
    match ALL the requested dimensions.

    Args:
      requested_dimensions: A dictionary containing the requested dimensions.
      machine_dimensions: A dictionary containing the machine's dimensions.

    Returns:
      True if the dimensions match, and False otherwise.
    """
    # An empty set of requested dimensions matches with anything...
    for (dimension_name, req_dimension_value) in requested_dimensions.items():
      # Dimension names must be strings... Our machine dimensions are unicode.
      if (not isinstance(dimension_name, (str, unicode)) or
          unicode(dimension_name) not in machine_dimensions):
        self.log_message('%s not in %s', dimension_name, machine_dimensions)
        return False
      # Now that we know the dimension is there, we must confirm it has
      # all the requested values, if there are more than one.
      machine_dimension_value = machine_dimensions[unicode(dimension_name)]
      if isinstance(req_dimension_value, (list, tuple)):
        for req_dimension_value_item in req_dimension_value:
          if (not isinstance(req_dimension_value_item, (str, unicode)) or
              unicode(req_dimension_value_item) not in machine_dimension_value):
            # TODO(user): We may want to have a separate log message for not str?
            self.log_message('%s not in %s', req_dimension_value_item,
                             machine_dimension_value)
            return False
      else:
        if (not isinstance(req_dimension_value, (str, unicode)) or
            unicode(req_dimension_value) not in machine_dimension_value):
          self.log_message('%s not in %s', req_dimension_value,
                           machine_dimension_value)
          return False
    # Assumed innocent until proven guilty... :-)
    return True

  def _RequestMachine(self, config_dimensions):
    """Reserves a specific machine for the caller and return it's ID.

    Args:
      config_dimensions: A configuration dimensions dictionary.
          See http://code.google.com/p/swarming/wiki/ConfigurationDimensions
          The key of the dict is a dimension name, like 'os' or 'browser' or
          'cpu'. The value of the map should be taken from the web page above,
          or at least match with the set specified in machine_list.

    Returns:
      A unique identifier for the machine being requested.
      -1 if machine list is invalid or can't be saved.
      -2 if no machine is available.
    """
    try:
      machine_list = self._LoadData()
    except (IOError, ValueError), e:
      self.log_error('Invalid machine list.\n%s' % str(e))
      return -1
    for machine in machine_list:
      # We use DONE to identify a machine that isn't used.
      if (machine['state'] == 'DONE' and
          self._MatchDimension(config_dimensions, machine['dimensions'])):
        # Mark the machine as READY now.
        machine['state'] = 'READY'
        try:
          self._SaveData(machine_list)
        except (IOError, ValueError), e:
          self.log_error('Can\'t save machine list.\n%s' % str(e))
          return -1
        self.log_message('Returning machine %s.', machine['id'])
        return machine['id']

    self.log_error('No machine available')
    return -2

  def _GetMachineInfo(self, machine_id):
    """Returns information about a specific machine.

    Args:
      machine_id: The unique identifier of the machine to get info for.

    Returns:
      (0, response) where response is a json encoded dictionary containing the
          state and ip of the machine.
      (-1, None) if machine list is invalid.
      (-3, None) if no machine with the given machine_id exists.
    """
    try:
      machine_list = self._LoadData()
    except (IOError, ValueError), e:
      self.log_error('Invalid machine list.\n%s' % str(e))
      return (-1, None)
    for machine in machine_list:
      if machine['id'] == machine_id:
        self.log_message('Returning info for machine %s.\n state: %s, ip: %s',
                         machine_id, machine['state'], machine['ip'])
        return (0, json.dumps({'status': machine['state'],
                               'host': machine['ip']}))

    self.log_error('Unknown machine %s' % machine_id)
    return (-3, None)

  def _ReleaseMachine(self, machine_id):
    """Releases the specified machine and make it available again.

    Args:
      machine_id: The unique identifier of the machine to be released.

    Returns:
      0 for success.
      -1 if machine list is invalid or can't be saved.
      -3 if no machine with the given machine_id exists.
    """
    try:
      machine_list = self._LoadData()
    except (IOError, ValueError), e:
      self.log_error('Invalid machine list.\n%s' % str(e))
      return -1
    for machine in machine_list:
      if machine['id'] == machine_id:
        # TODO(user): Check current state first?
        machine['state'] = 'DONE'
        try:
          self._SaveData(machine_list)
        except (IOError, ValueError), e:
          self.log_error('Can\'t save machine list.\n%s' % str(e))
          return -1
        self.log_message('Releasing machine %s.', machine_id)
        return 0

    self.log_error('Unknown machine %s' % machine_id)
    return -3

  def _ParseQuery(self, query):
    """Parses the query and handle the command it contains.

    Command results are writen in self.wfile in JSON format.

    Args:
      query: a dictionary of query arguments and their values.
    """
    if 'command' not in query or len(query['command']) != 1:
      self.send_error(400)
      return
    command = query['command'][0]
    if command == 'request':
      if 'dimensions' not in query or len(query['dimensions']) != 1:
        self.log_error('No dimensions specified!')
        self.send_error(400)
        return
      result = self._RequestMachine(
          config_dimensions=json.loads(query['dimensions'][0]))
      if result < 0:
        # TODO(user): Add error message.
        self.wfile.write(json.dumps({'available': 0}))
        return
      self.wfile.write(json.dumps({'available': 1, 'id': result}))
    elif command == 'info':
      if 'id' not in query or len(query['id']) != 1:
        self.send_error(400)
        return
      (result, response) = self._GetMachineInfo(
          machine_id=json.loads(query['id'][0]))
      if result < 0:
        self.wfile.write(json.dumps({'error': result}))
        return
      self.wfile.write(response)
    elif command == 'release':
      if 'id' not in query or len(query['id']) != 1:
        self.send_error(400)
        return
      result = self._ReleaseMachine(machine_id=json.loads(query['id'][0]))
      if result < 0:
        self.wfile.write(json.dumps({'error': result}))
      else:
        self.wfile.write(json.dumps({}))

  def do_GET(self):  #pylint: disable-msg=C6409
    """Receives a GET request and handle it."""
    parsed_request = urlparse.urlparse(self.path)
    if parsed_request.path != '/api':
      self.send_error(404)
      return
    self._ParseQuery(urlparse.parse_qs(parsed_request.query))

  def do_POST(self):  #pylint: disable-msg=C6409
    """Receives a POST request and handle it."""
    parsed_request = urlparse.urlparse(self.path)
    if parsed_request.path != '/api':
      self.send_error(404)
      return
    form = cgi.FieldStorage(
        fp=self.rfile, headers=self.headers,
        environ={'REQUEST_METHOD': 'POST',
                 'CONTENT_TYPE': self.headers['Content-Type']})
    self.send_response(200)
    self.end_headers()
    query = {}
    for field in form:
      query[field] = [form[field].value]
    return self._ParseQuery(query)

# TODO(user): Support command line arguments like --verbose.
if __name__ == '__main__':
  # Create the server and serve forever...
  MachineProviderServer.protocol_version = 'HTTP/1.0'
  # TODO(user): Add a command line argument to specify the file name and port.
  httpd = BaseHTTPServer.HTTPServer(('', 8001), MachineProviderServer)
  httpd.serve_forever()
