# Copyright 2014 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Utility functions when a script is run directly from a zip.

TODO(maruel): Copied from client/utils/zip_package.py, dedupe once bug 109 is
completed.
"""

import sys
import pkgutil
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
