#!/usr/bin/python2.7
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Implements the DimensionMapping class.

This class assists in storing what runner dimensions have been seen (for use in
queries) as well as converted a dimension hash back to the string that generated
it.
"""


from google.appengine.ext import ndb


class DimensionMapping(ndb.Model):
  """This class uses ndb so that app engine will automatically cache it.

  The key of this class is the hash of the dimension string.
  """
  # The raw config string.
  dimensions = ndb.StringProperty()
