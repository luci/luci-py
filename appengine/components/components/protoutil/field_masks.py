# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Utility functions for google.protobuf.field_mask_pb2.FieldMask.

Supports advanced field mask semantics:
- Refer to fields and map keys using . literals:
  - Supported map key types: string, integer types, bool.
  - Floating point (including double and float), enum, and bytes keys are not
    supported by protobuf or this implementation.
  - Fields: 'publisher.name' means field name of field publisher
  - string map keys: 'metadata.year' means string key 'year' of map field
    metadata
  - integer map keys (e.g. int32): 'year_ratings.0' means integer key 0 of a map
    field year_ratings
  - bool map keys: 'access_text.true' means boolean key true of a map field
    access_text
- String map keys that cannot be represented as an unquoted string literal,
  must be quoted using backticks: metadata.`year.published`, metadata.`17`,
  metadata.``. Backtick can be escaped with ``: a.`b``c` means map key "b`c"
  of map field a.
- Refer to all map keys using a * literal: "topics.*.archived" means field
  "archived" of all map values of map field "topic".
- Refer to all elements of a repeated field using a * literal: authors.*.name
- Refer to all fields of a message using * literal: publisher.*.
- Prohibit addressing a single element in repeated fields: authors.0.name

FieldMask.paths string grammar:
  path = segment {'.' segment}
  segment = literal | '*' | quoted_string;
  literal = string | integer | bool
  string = (letter | '_') {letter | '_' | digit}
  integer = ['-'] digit {digit};
  bool = 'true' | 'false';
  quoted_string = '`' { utf8-no-backtick | '``' } '`'

TODO(nodir): replace spec above with a link to a spec when it is available.
"""

import contextlib

from google import protobuf
from google.protobuf import descriptor

__all__ = [
    'EXCLUDE',
    'INCLUDE_ENTIRELY',
    'INCLUDE_PARTIALLY',
    'STAR',
    'include_field',
    'parse_segment_tree',
    'trim_message',
]


# Used in a parsed path to represent a star segment.
# See parse_segment_tree.
STAR = object()


def trim_message(message, field_tree):
  """Clears msg fields that are not in the field_mask.

  If field_mask is empty, this is a noop.

  Uses include_field to decide if a field should be cleared, see
  include_field's doc.

  Args:
    message: a google.protobuf.message.Message instance.
    field_tree: a field tree parsed using parse_segment_tree.
      The desciptor used in parse_segment_tree must match message.DESCRIPTOR.

  Raises:
    ValueError if a field mask path is invalid.
  """
  # Path to the current field value.
  seg_stack = []

  @contextlib.contextmanager
  def with_seg(seg):
    """Returns a context manager that adds/removes a segment.

    The context manager adds/removes the segment to the stack of segments on
    enter/exit. Yields a boolean indicating whether the segment must be
    included. See include_field() docstring for possible values.
    See parse_segment_tree for possible values of seg.
    """
    seg_stack.append(seg)
    try:
      yield include_field(field_tree, tuple(seg_stack))
    finally:
      seg_stack.pop()

  def trim_msg(msg):
    for f, v in msg.ListFields():
      with with_seg(f.name) as incl:
        if incl == INCLUDE_ENTIRELY:
          continue

        if incl == EXCLUDE:
          msg.ClearField(f.name)
          continue

        assert incl == INCLUDE_PARTIALLY

        if not f.message_type:
          # The field is scalar, but the field mask does not specify to include
          # it entirely. Skip it because scalars do not have subfields.
          # Note that parse_segment_tree would fail on such a mask because
          # a scalar field cannot be followed by other fields.
          msg.ClearField(f.name)
          continue

        # Trim the field value.
        if f.message_type.GetOptions().map_entry:
          for mk, mv in v.items():
            with with_seg(mk) as incl:
              if incl == INCLUDE_ENTIRELY:
                pass
              elif incl == EXCLUDE:
                v.pop(mk)
              elif isinstance(mv, protobuf.message.Message):
                trim_msg(mv)
              else:
                # The field is scalar, see the comment above.
                v.pop(mk)
        elif f.label == descriptor.FieldDescriptor.LABEL_REPEATED:
          with with_seg(STAR):
            for rv in v:
              trim_msg(rv)
        else:
          trim_msg(v)

  trim_msg(message)


EXCLUDE = 0
INCLUDE_PARTIALLY = 1
INCLUDE_ENTIRELY = 2


def include_field(field_tree, path):
  """Tells if a field value at the given path must be included in the response.

  Args:
    field_tree: a dict of fields, see parse_segment_tree.
    path: a tuple of path segments.

  Returns:
    EXCLUDE if the field value must be excluded.
    INCLUDE_PARTIALLY if some subfields of the field value must be included.
    INCLUDE_ENTIRELY if the field value must be included entirely.
  """
  assert isinstance(path, tuple), path
  assert path

  def include(node, i):
    """Tells if field path[i] must be included according to the node."""
    if not node:
      # node is a leaf.
      return INCLUDE_ENTIRELY

    if i == len(path):
      # n is an intermediate node and we've exhausted path.
      # Some of the value's subfields are included, so include this value
      # partially.
      return INCLUDE_PARTIALLY

    # Find children that match current segment.
    seg = path[i]
    children = [node.get(seg)]
    if seg != STAR:
      # node might have a star child
      # e.g. node = {'a': {'b': {}}, STAR: {'c': {}}}
      # If seg is 'x', we should check the star child.
      children.append(node.get(STAR))
    children = [c for c in children if c is not None]
    if not children:
      # Nothing matched.
      return EXCLUDE
    return max(include(c, i + 1) for c in children)

  return include(field_tree, 0)


def parse_segment_tree(field_mask, desc):
  """Parses a field mask to a tree of segments.

  Each node represents a segment and in turn is represented by a dict where each
  dict key is a child key and dict value is a child node. For example, parses
  ['a', 'b.c'] to {'a': {}, 'b': {'c': {}}}.

  Parses '*' segments as field_masks.STAR.
  Parses integer and boolean map keys as int and bool.

  Removes trailing stars, e.g. parses ['a.*'] to {'a': {}}.
  Removes redundant paths, e.g. parses ['a', 'a.b'] as {'a': {}}.

  Args:
    field_mask: a google.protobuf.field_mask_pb2.FieldMask instance.
    desc: a google.protobuf.descriptor.Descriptor for the target message.

  Raises:
    ValueError if a field path is invalid.
  """
  parsed_paths = []
  for p in field_mask.paths:
    try:
      parsed_paths.append(_parse_path(p, desc))
    except ValueError as ex:
      raise ValueError('invalid path "%s": %s' % (p, ex))

  parsed_paths = _normalize_paths(parsed_paths)

  root = {}
  for p in parsed_paths:
    node = root
    for seg in p:
      node = node.setdefault(seg, {})
  return root


def _normalize_paths(paths):
  """Normalizes field paths. Returns a new set of paths.

  paths must be parsed, see _parse_path.

  Removes trailing stars, e.g. convertes ('a', STAR) to ('a',).

  Removes paths that have a segment prefix already present in paths,
  e.g. removes ('a', 'b') from [('a', 'b'), ('a',)].
  """
  paths = _remove_trailing_stars(paths)
  return {
      p for p in paths
      if not any(p[:i] in paths for i in xrange(len(p)))
  }


def _remove_trailing_stars(paths):
  ret = set()
  for p in paths:
    assert isinstance(p, tuple), p
    if p[-1] == STAR:
      p = p[:-1]
    ret.add(p)
  return ret


# Token types.
_STAR, _PERIOD, _LITERAL, _STRING, _INTEGER, _UNKNOWN, _EOF = xrange(7)


_INTEGER_FIELD_TYPES = {
    descriptor.FieldDescriptor.TYPE_INT64,
    descriptor.FieldDescriptor.TYPE_INT32,
    descriptor.FieldDescriptor.TYPE_UINT32,
    descriptor.FieldDescriptor.TYPE_UINT64,
    descriptor.FieldDescriptor.TYPE_FIXED64,
    descriptor.FieldDescriptor.TYPE_FIXED32,
    descriptor.FieldDescriptor.TYPE_SFIXED64,
    descriptor.FieldDescriptor.TYPE_SFIXED32,
}
_SUPPORTED_MAP_KEY_TYPES = _INTEGER_FIELD_TYPES | {
    descriptor.FieldDescriptor.TYPE_STRING,
    descriptor.FieldDescriptor.TYPE_BOOL,
}


def _parse_path(path, desc):
  """Parses a field path to a tuple of segments.

  Grammar:
    path = segment {'.' segment}
    segment = literal | '*' | quoted_string;
    literal = string | integer | bool
    string = (letter | '_') {letter | '_' | digit}
    integer = ['-'] digit {digit};
    bool = 'true' | 'false';
    quoted_string = '`' { utf8-no-backtick | '``' } '`'

  Args:
    path: a field path.
    desc: a google.protobuf.descriptor.Descriptor of the target message.

  Returns:
    A tuple of segments. A star is returned as STAR object.

  Raises:
    ValueError if path is invalid.
  """
  tokens = list(_tokenize(path))
  ctx = _ParseContext(desc)
  peek = lambda: tokens[ctx.i]

  def read():
    tok = peek()
    ctx.i += 1
    return tok

  def read_path():
    segs = []
    while True:
      seg, must_be_last = read_segment()
      segs.append(seg)

      tok_type, tok = read()
      if tok_type == _EOF:
        break
      if must_be_last:
        raise ValueError('unexpected token "%s"; expected end of string' % tok)
      if tok_type != _PERIOD:
        raise ValueError('unexpected token "%s"; expected a period' % tok)
    return tuple(segs)

  def read_segment():
    """Returns (segment, must_be_last) tuple."""
    tok_type, tok = peek()
    assert tok
    if tok_type == _PERIOD:
      raise ValueError('a segment cannot start with a period')
    if tok_type == _EOF:
      raise ValueError('unexpected end')

    if ctx.expect_star:
      if tok_type != _STAR:
        raise ValueError('unexpected token "%s", expected a star' % tok)
      read()  # Swallow star.
      ctx.expect_star = False
      return STAR, False

    if ctx.desc is None:
      raise ValueError(
          'scalar field "%s" cannot have subfields' % ctx.field_path)

    if ctx.desc.GetOptions().map_entry:
      key_type = ctx.desc.fields_by_name['key'].type
      if key_type not in _SUPPORTED_MAP_KEY_TYPES:
        raise ValueError(
            'unsupported key type of field "%s"' % ctx.field_path)
      if tok_type == _STAR:
        read()  # Swallow star.
        seg = STAR
      elif key_type == descriptor.FieldDescriptor.TYPE_BOOL:
        seg = read_bool()
      elif key_type in _INTEGER_FIELD_TYPES:
        seg = read_integer()
      else:
        assert key_type == descriptor.FieldDescriptor.TYPE_STRING
        seg = read_string()

      ctx.advance_to_field(ctx.desc.fields_by_name['value'])
      return seg, False

    if tok_type == _STAR:
      # Include all fields.
      read()  # Swallow star.
       # A STAR field cannot be followed by subfields.
      return STAR, True

    if tok_type != _LITERAL:
      raise ValueError(
          'unexpected token "%s"; expected a field name' % tok)
    read()  # Swallow field name.
    field_name = tok

    field = ctx.desc.fields_by_name.get(field_name)
    if field is None:
      prefix = ctx.field_path
      full_name = '%s.%s' % (prefix, field_name) if prefix else field_name
      raise ValueError('field "%s" does not exist' % full_name)
    ctx.advance_to_field(field)
    return field_name, False

  def read_bool():
    tok_type, tok = read()
    if tok_type != _LITERAL or tok not in ('true', 'false'):
      raise ValueError(
          'unexpected token "%s", expected true or false' % tok)
    return tok == 'true'

  def read_integer():
    tok_type, tok = read()
    if tok_type != _INTEGER:
      raise ValueError('unexpected token "%s"; expected an integer' % tok)
    return int(tok)

  def read_string():
    tok_type, tok = read()
    if tok_type not in (_LITERAL, _STRING):
      raise ValueError('unexpected token "%s"; expected a string' % tok)
    return tok

  return read_path()


class _ParseContext(object):
  """Context of parsing in _parse_path."""

  def __init__(self, desc):
    self.i = 0
    self.desc = desc
    self.expect_star = False
    self._field_path = []  # full path of the current field

  def advance_to_field(self, field):
    """Advances the context to the next message field.

    Args:
      field: a google.protobuf.descriptor.FieldDescriptor to move to.
    """
    self.desc = field.message_type
    self.expect_star = (
        field.label == descriptor.FieldDescriptor.LABEL_REPEATED
        and not (self.desc and self.desc.GetOptions().map_entry))
    self._field_path.append(field.name)

  @property
  def field_path(self):
    return '.'.join(self._field_path)


def _tokenize(path):
  """Transforms path to an iterator of (token_type, string) tuples.

  Raises:
    ValueError if a quoted string is not closed.
  """
  assert isinstance(path, basestring), path

  i = 0

  while i < len(path):
    start = i
    c = path[i]
    i += 1
    if c == '`':
      quoted_string = []  # Parsed quoted string as list of string parts.
      while True:
        next_backtick = path.find('`', i)
        if next_backtick == -1:
          raise ValueError('a quoted string is not closed')

        quoted_string.append(path[i:next_backtick])
        i = next_backtick + 1  # Swallow the discovered backtick.

        escaped_backtick = i < len(path) and path[i] == '`'
        if not escaped_backtick:
          break
        quoted_string.append('`')
        i += 1  # Swallow second backtick.

      yield (_STRING, ''.join(quoted_string))
    elif c == '*':
      yield (_STAR, c)
    elif c == '.':
      yield (_PERIOD, c)
    elif c == '-' or c.isdigit():
      while i < len(path) and path[i].isdigit():
        i += 1
      yield (_INTEGER, path[start:i])
    elif c == '_' or c.isalpha():
      while i < len(path) and (path[i].isalnum() or path[i] == '_'):
        i += 1
      yield (_LITERAL, path[start:i])
    else:
      yield (_UNKNOWN, c)
  yield (_EOF, '<eof>')
