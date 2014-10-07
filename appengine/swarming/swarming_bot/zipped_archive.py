# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utility functions when a script is run directly from a zip.

TODO(maruel): Copied from client/utils/zip_package.py, dedupe once bug 109 is
completed.
"""

import hashlib
import pkgutil
import sys
import zipfile
import zipimport


def get_module_zip_archive(module):
  """Given a module, returns path to a zip package that contains it or None."""
  loader = pkgutil.get_loader(module)
  if not isinstance(loader, zipimport.zipimporter):
    return None
  # 'archive' property is documented only for python 2.7, but it appears to be
  # there at least since python 2.5.2.
  return loader.archive


def get_main_script_path():
  """If running from zip returns path to a zip file, else path to __main__.

  Basically returns path to a file passed to python for execution
  as in 'python <main_script>' considering a case of executable zip package.

  Returns path relative to a current directory of when process was started.
  """
  # If running from interactive console __file__ is not defined.
  main = sys.modules['__main__']
  return get_module_zip_archive(main) or getattr(main, '__file__', None)


def generate_version():
  """Generates the sha-1 based on the content of this zip."""
  result = hashlib.sha1()
  # TODO(maruel): This function still has to be compatible with python 2.6. Use
  # a with statement once every slaves are upgraded to 2.7.
  z = zipfile.ZipFile(get_main_script_path(), 'r')
  for item in sorted(z.namelist()):
    f = z.open(item)
    result.update(item)
    result.update('\x00')
    result.update(f.read())
    result.update('\x00')
    f.close()
  z.close()
  return result.hexdigest()
