# Copyright 2013 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Mixed bag of NDB utilities."""

# Disable 'Access to a protected member ...'. NDB uses '_' for other purposes.
# pylint: disable=W0212

# Disable: 'Method could be a function'. It can't: NDB expects a method.
# pylint: disable=R0201

import datetime
import functools
import json
import os
import threading
import time

from email import utils

from google.appengine.api import memcache
from google.appengine.api import modules
from google.appengine.ext import ndb


DATETIME_FORMAT = u'%Y-%m-%d %H:%M:%S'
DATE_FORMAT = u'%Y-%m-%d'
VALID_DATETIME_FORMATS = ('%Y-%m-%d', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S')


def is_local_dev_server():
  """Returns True if running on local development server."""
  return os.environ.get('SERVER_SOFTWARE', '').startswith('Development')


def to_units(number):
  """Convert a string to numbers."""
  UNITS = ('', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y')
  unit = 0
  while number >= 1024.:
    unit += 1
    number = number / 1024.
    if unit == len(UNITS) - 1:
      break
  if unit:
    return '%.2f%s' % (number, UNITS[unit])
  return '%d' % number


def get_request_as_int(request, key, default, min_value, max_value):
  """Returns a request value as int."""
  value = request.params.get(key, '')
  try:
    value = int(value)
  except ValueError:
    return default
  return min(max_value, max(min_value, value))


def get_request_as_datetime(request, key):
  value_text = request.params.get(key)
  if value_text:
    for f in VALID_DATETIME_FORMATS:
      try:
        return datetime.datetime.strptime(value_text, f)
      except ValueError:
        continue
  return None


def datetime_to_rfc2822(dt):
  """datetime -> string value for Last-Modified header as defined by RFC2822."""
  assert dt.tzinfo is None, 'Expecting UTC timestamp: %s' % dt
  return utils.formatdate(datetime_to_timestamp(dt) / 1000000.0)


### Cache


class _Cache(object):
  """Holds state of a cache for cache_with_expiration and cache decorators."""

  def __init__(self, func, expiration_sec):
    self.func = func
    self.expiration_sec = expiration_sec
    self.lock = threading.Lock()
    self.value = None
    self.value_is_set = False
    self.expires = None

  def get_value(self):
    """Returns a cached value refreshing it if it has expired."""
    with self.lock:
      if not self.value_is_set or (self.expires and time.time() > self.expires):
        self.value = self.func()
        self.value_is_set = True
        if self.expiration_sec:
          self.expires = time.time() + self.expiration_sec
      return self.value

  def get_wrapper(self):
    """Returns a callable object that can be used in place of |func|.

    It's basically self.get_value, updated by functools.wraps to look more like
    original function.
    """
    # functools.wraps doesn't like 'instancemethod', use lambda as a proxy.
    # pylint: disable=W0108
    return functools.wraps(self.func)(lambda: self.get_value())


def cache(func):
  """Decorator that implements permanent cache of a zero-parameter function."""
  return _Cache(func, None).get_wrapper()


def cache_with_expiration(expiration_sec):
  """Decorator that implements in-memory cache for a zero-parameter function."""
  def decorator(func):
    return _Cache(func, expiration_sec).get_wrapper()
  return decorator


def constant_time_equals(a, b):
  """Compares two strings in constant time regardless of theirs content."""
  if len(a) != len(b):
    return False
  result = 0
  for x, y in zip(a, b):
    result |= ord(x) ^ ord(y)
  return result == 0


def get_module_version_list(module_list, tainted):
  """Returns a list of pairs (module name, version name) to fetch logs for.

  Arguments:
    module_list: list of modules to list, defaults to all modules.
    tainted: if False, excludes versions with '-tainted' in their name.
  """
  result = []
  if not module_list:
    # If the function it called too often, it'll raise a OverQuotaError. So
    # cache it for 10 minutes.
    module_list = memcache.get('modules_list')
    if not module_list:
      module_list = modules.get_modules()
      memcache.set('modules_list', module_list, time=10*60)

  for module in module_list:
    # If the function it called too often, it'll raise a OverQuotaError.
    # Versions is a bit more tricky since we'll loose data, since versions are
    # changed much more often than modules. So cache it for 1 minute.
    key = 'modules_list-' + module
    version_list = memcache.get(key)
    if not version_list:
      version_list = modules.get_versions(module)
      memcache.set(key, version_list, time=60)
    result.extend(
        (module, v) for v in version_list if tainted or '-tainted' not in v)
  return result


## JSON


def to_json_encodable(data):
  """Converts data into json-compatible data."""
  if isinstance(data, unicode) or data is None:
    return data
  if isinstance(data, str):
    return data.decode('utf-8')
  if isinstance(data, (int, float, long)):
    # Note: overflowing is an issue with int and long.
    return data
  if isinstance(data, (list, set, tuple)):
    return [to_json_encodable(i) for i in data]
  if isinstance(data, dict):
    assert all(isinstance(k, basestring) for k in data), data
    return {
      to_json_encodable(k): to_json_encodable(v) for k, v in data.iteritems()
    }

  if isinstance(data, datetime.datetime):
    # Convert datetime objects into a string, stripping off milliseconds. Only
    # accept naive objects.
    if data.tzinfo is not None:
      raise ValueError('Can only serialize naive datetime instance')
    return data.strftime(DATETIME_FORMAT)
  if isinstance(data, datetime.date):
    return data.strftime(DATE_FORMAT)
  if isinstance(data, datetime.timedelta):
    # Convert timedelta into seconds, stripping off milliseconds.
    return int(data.total_seconds())

  if hasattr(data, 'to_dict') and callable(data.to_dict):
    # This takes care of ndb.Model.
    return to_json_encodable(data.to_dict())
  assert False, 'Don\'t know how to handle %r' % data


def encode_to_json(data):
  """Converts any data as a json string."""
  return json.dumps(
      to_json_encodable(data),
      sort_keys=True,
      separators=(',', ':'),
      encoding='utf-8')


# Use field when converting entity to a serializable dict.
READABLE = 1 << 0
# Use field when converting entity from a serializable dict.
WRITABLE = 1 << 1


class SerializableModelMixin(object):
  """Mixing for entity that can convert itself to/from serializable dictionary.

  A serializable dictionary trivially can be converted to/from JSON, XML, YAML,
  etc. via standard serializers (e.g json.dump and json.load).

  A serializable dictionary is a dictionary with string keys and values that are
    * Scalar types: int, long, float.
    * String types: str, unicode.
    * Sequences: list, tuple.
    * Another serializable dictionaries.
  """

  # Dictionary: property name -> bit mask with READABLE and\or WRITABLE flag.
  # It defines what properties to use when convert an entity to or from
  # serializable dict. See doc strings for 'to_serializable_dict' and
  # 'convert_serializable_dict' for more details.
  # Default is None, which means that all defined properties are readable
  # and writable.
  serializable_properties = None

  def to_serializable_dict(self, with_id_as=None, exclude=None):
    """Converts this entity to a serializable dictionary.

    Operates only on properties that have READABLE flag set in
    |serializable_properties|. All other entity properties are effectively
    invisible.

    Args:
      with_id_as: name of the optional dict key to put entity's string_id() to.
      exclude: list of fields to exclude from the dict.
    """
    # TODO(vadimsh): Add 'include' and 'exclude' support when needed.
    conv = _ModelDictConverter(
        property_converters=_rich_to_simple_converters,
        field_mode_predicate=lambda mode: bool(mode & READABLE))
    serializable_dict = conv.convert_dict(
        self.__class__, self.to_dict(exclude=exclude))
    if with_id_as:
      assert isinstance(with_id_as, basestring)
      serializable_dict[with_id_as] = self.key.string_id()
    return serializable_dict

  @classmethod
  def from_serializable_dict(cls, serializable_dict):
    """Makes a new entity with properties populated from |serializable_dict|.

    See doc string for 'convert_serializable_dict' method for more details.
    """
    return cls(**cls.convert_serializable_dict(serializable_dict))

  @classmethod
  def convert_serializable_dict(cls, serializable_dict):
    """Converts a serializable dictionary to dictionary with rich-typed values.

    It can then be used in entity constructor or in 'populate' method. This
    method works as reverse of to_serializable_dict, in particular if all
    fields are readable and writable the following holds:

    ent = Entity(...)
    assert ent == Entity(
        **Entity.convert_serializable_dict(ent.to_serializable_dict()))

    Operates only on properties that have WRITABLE flag set in
    |serializable_properties|. All other keys from |serializable_dict|
    (i.e. ones that don't match any entity properties at all or ones that match
    properties not explicitly marked as WRITABLE) are silently ignored.
    """
    conv = _ModelDictConverter(
        property_converters=_simple_to_rich_converters,
        field_mode_predicate=lambda mode: bool(mode & WRITABLE))
    return conv.convert_dict(cls, serializable_dict)


class _ModelDictConverter(object):
  """Uses |property_converters| to recursively convert dictionary values.

  Works by simultaneously walking over dict and and entity's structure. Dict
  is used for actual values, entity is used for typing information.

  Used for conversion in both directions: rich-typed dict to serializable dict
  and vice versa. The difference is in |property_converters| used.

  For example when converting in 'rich-typed to serializable dict' direction,
  |property_converters| contains functions that take rich types (e.g. datetime)
  and produce simple types (e.g int with timestamp). For reverse direction,
  |property_converters| contain functions that perform reverse conversion
  (e.g. int timestamp -> datetime).
  """

  def __init__(self, property_converters, field_mode_predicate):
    """Args:
      property_converters: sequence of tuples that define how to handle various
        NDB property classes.
      field_mode_predicate: callable that will be used to decide what properties
        to use during conversion. It is called with single integer argument
        |mode| which is a value from entity.serializable_properties dictionary
        that correspond to property being considered. If |field_mode_predicate|
        returns True, then property will be used, otherwise it will be silently
        ignored during conversion (i.e. resulting dict will not have it even
        if it was present in incoming dict).

    Each property converter tuple has 3 components:
      * ndb.Property subclass this converter applies to.
      * Boolean: True to apply converter to all subclasses or False only to
        this specific class.
      * Actual converter: function(property instance, from type) -> to type.

    For instance when converting rich-typed dict to serializable dict, converter
    for DateTimeProperty will be defined as:
      (
        ndb.DateTimeProperty,
        False,
        lambda(ndb.DateTimeProperty instance, datetime) -> integer
      )
    """
    self.property_converters = property_converters
    self.field_mode_predicate = field_mode_predicate

  def convert_dict(self, model_cls, model_dict):
    """Returns new dictionary with values converted using |property_converters|.

    Args:
      model_cls: ndb.Model subclass that acts as a schema with type information,
        its model_cls._properties will be used as a source of typing information
        for corresponding keys in |model_dict|.
      model_dict: dictionary that has same structure as entity defined by
        model_cls. Its values will be passed through appropriate property
        converters to get values in returned dict.

    Returns:
      New dictionary that structurally is a subset of |model_dict|, but with
      values of different type (defined by |property_converters|).
    """
    assert isinstance(model_dict, dict)
    allowed_properties = self.get_allowed_properties(model_cls)
    result = {}
    for key, value in model_dict.iteritems():
      if allowed_properties is None or key in allowed_properties:
        result[key] = self.convert_property(model_cls._properties[key], value)
    return result

  def convert_property(self, prop, value):
    """Converts value of a single key.

    Args:
      prop: instance of ndb.Property subclass that defines typing information.
      values: incoming property value to transform.

    Returns:
      Transformed property value that should be used in resulting dictionary.
      Uses |prop| and |property_converters| to figure out how to perform the
      conversion.
    """
    if value is None:
      return None
    converter = self.get_property_converter(prop)
    if prop._repeated:
      assert isinstance(value, list)
      return [converter(prop, x) for x in value]
    else:
      return converter(prop, value)

  def get_allowed_properties(self, model_cls):
    """Returns a set of property names to consider when converting a dictionary.

    When working with StructuredProperty based on regular ndb.Model, export all
    fields. Otherwise use model_cls.serializable_properties and
    self.field_mode_predicate to figure out set of properties to use.

    Return value of None means all defined properties should be used.
    """
    assert issubclass(model_cls, ndb.Model)
    assert not issubclass(model_cls, ndb.Expando), 'Expando is not supported'
    if not issubclass(model_cls, SerializableModelMixin):
      return None
    if model_cls.serializable_properties is None:
      return None
    return set(
        field for field, mode in model_cls.serializable_properties.iteritems()
        if self.field_mode_predicate(mode))

  def get_property_converter(self, prop):
    """Returns callable that can convert values corresponding to ndb property.

    Args:
      prop: instance of ndb.Property subclass that defines typing information.

    Returns:
      Callable (property instance, incoming value) -> converter values.
    """
    # For structured properties, recursively call convert_dict.
    if isinstance(prop, (ndb.StructuredProperty, ndb.LocalStructuredProperty)):
      return lambda prop, x: self.convert_dict(prop._modelclass, x)
    # For other properties consult the registry of converters.
    for prop_cls, include_subclasses, conv in self.property_converters:
      if (include_subclasses and isinstance(prop, prop_cls) or
          not include_subclasses and type(prop) == prop_cls):
        return conv
    # Give up.
    raise TypeError('Don\'t know how to work with %s' % type(prop).__name__)


################################################################################
## Registry of property converters.


_rich_to_simple_converters = []
_simple_to_rich_converters = []


def register_converter(
    property_cls, include_subclasses, rich_to_simple, simple_to_rich):
  """Register a pair of functions that can convert some ndb.Property subclass.

  Used by ndb.Model, utils.SerializableModelMixin to convert entities to
  serializable dicts and vice versa.

  Args:
    property_cls: ndb.Property subclass.
    include_subclasses: True to apply this converter to all subclasses as well.
    rich_to_simple: function that converts property's value type to some simple
        type: rich_to_simple(property_instance, property_value) -> simple_value.
    simple_to_rich: function that converts some simple type to property's value
        type: simple_to_rich(property_instance, simple_value) -> property_value.
  """
  assert issubclass(property_cls, ndb.Property)
  _rich_to_simple_converters.append(
      (property_cls, include_subclasses, rich_to_simple))
  _simple_to_rich_converters.append(
      (property_cls, include_subclasses, simple_to_rich))


################################################################################
## Register trivial properties.


# Properties with values that look exactly the same in to_dict() and
# in to_serializable_dict() representations.
SIMPLE_PROPERTIES = (
  ndb.BlobProperty,
  ndb.BooleanProperty,
  ndb.FloatProperty,
  ndb.IntegerProperty,
  ndb.JsonProperty,
  ndb.PickleProperty,
  ndb.StringProperty,
  ndb.TextProperty,
)


def _register_simple_converters():
  noop = lambda _prop, x: x
  for simple_prop_cls in SIMPLE_PROPERTIES:
    register_converter(
        property_cls=simple_prop_cls,
        include_subclasses=False,
        rich_to_simple=noop,
        simple_to_rich=noop)


_register_simple_converters()


################################################################################
## DateTime properties.


# UTC datetime corresponding to zero Unix timestamp.
_EPOCH = datetime.datetime.utcfromtimestamp(0)


def datetime_to_timestamp(value):
  """Converts UTC datetime to integer timestamp in microseconds since epoch."""
  if value.tzinfo is not None:
    raise ValueError('Only UTC datetime is supported')
  dt = value - _EPOCH
  return dt.microseconds + 1000 * 1000 * (dt.seconds + 24 * 3600 * dt.days)


def timestamp_to_datetime(value):
  """Converts integer timestamp in microseconds since epoch to UTC datetime."""
  return _EPOCH + datetime.timedelta(microseconds=value)


# TODO(vadimsh): Add ndb.DateProperty if needed.
register_converter(
    property_cls=ndb.DateTimeProperty,
    include_subclasses=False,
    rich_to_simple=lambda _prop, x: datetime_to_timestamp(x),
    simple_to_rich=lambda _prop, x: timestamp_to_datetime(x))


################################################################################
## BytesSerializable properties.


class BytesSerializable(object):
  """Interface that defines to_bytes() and from_bytes() methods.

  Objects that implement this interface know how to serialize/deserialize
  themselves to/from bytes array (represented by 'str').
  """

  def to_bytes(self):
    """Serialize this object to byte array."""
    raise NotImplementedError()

  @classmethod
  def from_bytes(cls, byte_buf):
    """Deserialize byte array into new instance of the class."""
    raise NotImplementedError()


class BytesSerializableProperty(ndb.BlobProperty):
  """BlobProperty that uses values's to_bytes/from_bytes methods.

  Property will use to_bytes() to serialize an object before storing it in
  DB and from_bytes() when fetching it from DB and validating.

  Usage:
    class MyValue(BytesSerializable):
      ...

    class MyValueProperty(BytesSerializableProperty):
      _value_type = MyValue

    class Model(ndb.Model):
      my_value = MyValueProperty()
  """

  # Should be set in subclasses to some BytesSerializable subclass that this
  # property class will represent.
  _value_type = None

  def _validate(self, value):
    if not isinstance(value, self._value_type):
      raise TypeError(
          'Expecting %s, got %r' % (self._value_type.__name__, value))

  def _to_base_type(self, value):
    result = value.to_bytes()
    assert isinstance(result, str)
    return result

  def _from_base_type(self, value):
    assert isinstance(value, str)
    result = self._value_type.from_bytes(value)
    assert isinstance(result, self._value_type)
    return result


# Handles all property classes inherited from BytesSerializableProperty.
register_converter(
    property_cls=BytesSerializableProperty,
    include_subclasses=True,
    rich_to_simple=lambda prop, value: value.to_bytes(),
    simple_to_rich=lambda prop, value: prop._value_type.from_bytes(value))


################################################################################
## JsonSerializable properties.


class JsonSerializable(object):
  """Interface that defines to_jsonish() and from_jsonish() methods.

  Value is 'jsonish' if it can be converted to JSON with standard json.dump.

  Objects that implement this interface know how to convert themselves to/from
  jsonish values (usually dicts but not necessarily).
  """

  def to_jsonish(self):
    """Convert this object to jsonish value."""
    raise NotImplementedError()

  @classmethod
  def from_jsonish(cls, obj):
    """Given jsonish value convert it to new instance of the class."""
    raise NotImplementedError()


class JsonSerializableProperty(ndb.JsonProperty):
  """JsonProperty that uses values's to_jsonish/from_jsonish methods.

  Property will use to_jsonish() to convert an object to simple JSONish value
  before storing it in DB as JSON and from_jsonish() when fetching it from
  DB and validating.

  Usage:
    class MyValue(JsonSerializable):
      ...

    class MyValueProperty(JsonSerializableProperty):
      _value_type = MyValue

    class Model(ndb.Model):
      my_value = MyValueProperty()
  """

  # Should be set in subclasses to some JsonSerializable subclass that this
  # property class will represent.
  _value_type = None

  def _validate(self, value):
    if not isinstance(value, self._value_type):
      raise TypeError(
          'Expecting %s, got %r' % (self._value_type.__name__, value))

  def _to_base_type(self, value):
    return value.to_jsonish()

  def _from_base_type(self, value):
    return self._value_type.from_jsonish(value)


# Handles all property classes inherited from JsonSerializableProperty.
register_converter(
    property_cls=JsonSerializableProperty,
    include_subclasses=True,
    rich_to_simple=lambda prop, value: value.to_jsonish(),
    simple_to_rich=lambda prop, value: prop._value_type.from_jsonish(value))


def pop_future_done(futures):
  """Removes the currently done futures."""
  for i in xrange(len(futures) - 1, -1, -1):
    if futures[i].done():
      futures.pop(i)


def page_queries(queries, fetch_page_size=20):
  """Yields all the items returned by the queries, page by page.

  It makes heavy use of fetch_page_async() for maximum efficiency.
  """
  queries = queries[:]
  futures = [q.fetch_page_async(fetch_page_size) for q in queries]
  while queries:
    i = futures.index(ndb.Future.wait_any(futures))
    results, cursor, more = futures[i].get_result()
    if not more:
      # Remove completed queries.
      queries.pop(i)
      futures.pop(i)
    else:
      futures[i] = queries[i].fetch_page_async(
          fetch_page_size, start_cursor=cursor)
    yield results


def _process_chunk_of_items(
    map_fn, action_futures, items_to_process, max_inflight, map_page_size):
  """Maps as many items as possible and throttles down to 'max_inflight'.

  |action_futures| is modified in-place.
  Remaining items_to_process is returned.
  """
  # First, throttle.
  pop_future_done(action_futures)
  while len(action_futures) > max_inflight:
    ndb.Future.wait_any(action_futures)
    pop_future_done(action_futures)

  # Then, map. map_fn() may return None so "or []" to not throw an exception. It
  # just means there no async operation to wait on.
  action_futures.extend(map_fn(items_to_process[:map_page_size]) or [])
  return items_to_process[map_page_size:]


def incremental_map(
    queries, map_fn, filter_fn=None, max_inflight=100, map_page_size=20,
    fetch_page_size=20):
  """Applies |map_fn| to objects in a list of queries asynchrously.

  This function is itself synchronous.

  It's a mapper without a reducer.

  Arguments:
    queries: list of iterators of items to process.
    map_fn: callback that accepts a list of objects to map and optionally
            returns a list of ndb.Future.
    filter_fn: optional callback that can filter out items from |query| from
               deletion when returning False.
    max_inflight: maximum limit of number of outstanding futures returned by
                  |map_fn|.
    map_page_size: number of items to pass to |map_fn| at a time.
    fetch_page_size: number of items to retrieve from |queries| at a time.
  """
  items_to_process = []
  action_futures = []

  for items in page_queries(queries, fetch_page_size=fetch_page_size):
    items_to_process.extend(i for i in items if not filter_fn or filter_fn(i))
    while len(items_to_process) >= map_page_size:
      items_to_process = _process_chunk_of_items(
          map_fn, action_futures, items_to_process, max_inflight, map_page_size)

  while items_to_process:
    items_to_process = _process_chunk_of_items(
        map_fn, action_futures, items_to_process, max_inflight, map_page_size)

  ndb.Future.wait_all(action_futures)
