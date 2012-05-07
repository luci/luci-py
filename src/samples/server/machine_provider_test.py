#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Test cases for the machine provider code."""

import BaseHTTPServer
try:
  import simplejson as json  # pylint: disable-msg=C6204
except ImportError:
  import json  # pylint: disable-msg=C6204
import logging  # pylint: disable-msg=C6204
import optparse
import os
import tempfile
import threading
import unittest

import machine_provider
import machine_provider_server
from server import base_machine_provider


def RunMachineProviderServer(http_server, started_event):
  logging.debug('starting server')
  started_event.set()
  http_server.serve_forever()
  logging.debug('exited server')


class _MachineProviderTestRunner(unittest.TestCase):

  def setUp(self):
    (unused_file_descriptor, self._tempfile_name) = tempfile.mkstemp()
    os.close(unused_file_descriptor)
    started_event = threading.Event()
    machine_provider_server.MachineProviderServer.machine_list_filename = (
        self._tempfile_name)
    machine_provider_server.MachineProviderServer.verbose = (
        _MachineProviderTestProgram.options.verbose)
    machine_provider_server.MachineProviderServer.protocol_version = 'HTTP/1.0'
    self._http_server = BaseHTTPServer.HTTPServer(
        ('', 8001), machine_provider_server.MachineProviderServer)
    self._thread = threading.Thread(target=RunMachineProviderServer,
                                    args=(self._http_server, started_event))
    logging.debug('starting thread')
    self._thread.start()
    logging.debug('started thread')

    self._machine_provider = machine_provider.MachineProvider()

    started_event.wait()

  def tearDown(self):
    self._http_server.shutdown()
    self._http_server.server_close()
    self._http_server = None
    logging.debug('joining thread')
    self._thread.join()
    logging.debug('joined thread')
    self._thread = None
    if os.path.exists(self._tempfile_name):
      os.remove(self._tempfile_name)

  def _SaveData(self, data):
    try:
      machine_list_file = open(self._tempfile_name, 'w')
      logging.debug('dumping: %s', str(data))
      data_str = json.dump(data, machine_list_file)
      logging.debug('dumped to file: %s\n%s', self._tempfile_name, data_str)
      machine_list_file.close()
    except (IOError, ValueError), e:
      logging.exception(e)
      if machine_list_file:
        machine_list_file.close()
      raise

  def _GetDefaultTestData(self):
    return [{'id': 'win-xp-ready',
             'ip': 'win-xp-ready',
             'state': 'ACQUIRED',
             'dimensions': {'os': ['win-xp'],
                            'cpu': ['intel'],
                            'browser': ['ie-7'],
                            'lang': ['en-US']}},
            {'id': 'win-xp-done',
             'ip': 'win-xp-done',
             'state': 'AVAILABLE',
             'dimensions': {'os': ['win-xp'],
                            'cpu': ['intel'],
                            'browser': ['ie-7'],
                            'lang': ['en-US']}},
            {'id': 'vista-ready',
             'ip': 'vista-ready',
             'state': 'ACQUIRED',
             'dimensions': {'os': ['vista'],
                            'cpu': ['intel'],
                            'browser': ['ie-7'],
                            'lang': ['en-US']}},
            {'id': 'vista-done',
             'ip': 'vista-done',
             'state': 'AVAILABLE',
             'dimensions': {'os': ['vista'],
                            'cpu': ['intel'],
                            'browser': ['ie-7'],
                            'lang': ['en-US']}},
            {'id': 'vista-fr-done',
             'ip': 'vista-fr-done',
             'state': 'AVAILABLE',
             'dimensions': {'os': ['vista'],
                            'cpu': ['intel'],
                            'browser': ['ie-7'],
                            'lang': ['fr']}},
            {'id': 'vista-ie7-ff-done',
             'ip': 'vista-ie7-ff-done',
             'state': 'AVAILABLE',
             'dimensions': {'os': ['vista'],
                            'cpu': ['intel'],
                            'browser': ['ie-7', 'ff'],
                            'lang': ['en-US']}}]

  def testRequestRelease(self):
    logging.debug('Testing simple cases of the RequestMachine method')
    self._SaveData(self._GetDefaultTestData())
    dimensions = {'os': 'win-xp'}
    unused_pool = None
    unused_lifespan = None
    machine_id = self._machine_provider.RequestMachine(unused_pool, dimensions,
                                                       unused_lifespan)
    self.assertEqual('win-xp-done', machine_id)

    self.assertRaises(base_machine_provider.MachineProviderException,
                      self._machine_provider.RequestMachine,
                      unused_pool, dimensions, unused_lifespan)

    self._machine_provider.ReleaseMachine(machine_id)

    dimensions = {'os': 'vista', 'browser': 'ff'}
    machine_id = self._machine_provider.RequestMachine(unused_pool, dimensions,
                                                       unused_lifespan)
    self.assertEqual('vista-ie7-ff-done', machine_id)

    self._machine_provider.ReleaseMachine(machine_id)

    dimensions = {'os': 'vista', 'lang': 'fr'}
    machine_id = self._machine_provider.RequestMachine(unused_pool, dimensions,
                                                       unused_lifespan)
    self.assertEqual('vista-fr-done', machine_id)

    self._machine_provider.ReleaseMachine(machine_id)

  def testGetMachineInfo(self):
    logging.debug('Testing simple cases of the RequestMachine method')
    self._SaveData(self._GetDefaultTestData())

    machine_info = self._machine_provider.GetMachineInfo('win-xp-done')
    self.assertEqual(machine_info.Status(),
                     base_machine_provider.MachineStatus.AVAILABLE)
    self.assertEqual(machine_info.Host(), 'win-xp-done')

    machine_info = self._machine_provider.GetMachineInfo('win-xp-ready')
    self.assertEqual(machine_info.Status(),
                     base_machine_provider.MachineStatus.ACQUIRED)
    self.assertEqual(machine_info.Host(), 'win-xp-ready')

    machine_info = self._machine_provider.GetMachineInfo('vista-done')
    self.assertEqual(machine_info.Status(),
                     base_machine_provider.MachineStatus.AVAILABLE)
    self.assertEqual(machine_info.Host(), 'vista-done')

    machine_info = self._machine_provider.GetMachineInfo('vista-ie7-ff-done')
    self.assertEqual(machine_info.Status(),
                     base_machine_provider.MachineStatus.AVAILABLE)
    self.assertEqual(machine_info.Host(), 'vista-ie7-ff-done')


class _MachineProviderTestProgram(unittest.TestProgram):
  options = None
  _DESCRIPTION = ('This script starts tests for the machine procider server.')

  def parseArgs(self, argv):
    parser = optparse.OptionParser(
        usage='%prog [options] [filename]',
        description=_MachineProviderTestProgram._DESCRIPTION)
    parser.add_option('-v', '--verbose', action='store_true',
                      help='Set logging level to INFO. Optional. Defaults to '
                      'ERROR level.')
    parser.add_option('-o', '--xmlrpc_server_start_timeout',
                      dest='xmlrpc_server_start_timeout', type=int,
                      help='How long should we wait (in seconds) for the '
                      'XmlRpc server to start? Defaults to 60 seconds.')
    parser.set_default('xmlrpc_server_start_timeout', 60)

    (_MachineProviderTestProgram.options, other_args) = parser.parse_args(
        argv[1:])
    if _MachineProviderTestProgram.options.verbose:
      logging.getLogger().setLevel(logging.DEBUG)
    else:
      logging.getLogger().setLevel(logging.ERROR)
    super(_MachineProviderTestProgram, self).parseArgs(other_args)


if __name__ == '__main__':
  _MachineProviderTestProgram()
