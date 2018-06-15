#!/usr/bin/env python

# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

import sys
import unittest

from test_support import test_env
test_env.setup_test_env()

from test_support import test_case
import partial


class MergeTestCase(test_case.TestCase):
  """Tests for partial._merge."""

  def test_empty(self):
    d = {}
    partial._merge({}, d)
    self.assertFalse(d)

    d = {'a': {}}
    partial._merge({}, d)
    self.assertEqual(d, {
      'a': {},
    })

  def test_simple(self):
    d = {}
    partial._merge({'a': {}}, d)
    self.assertEqual(d, {
      'a': {},
    })

    d = {'a': {}}
    partial._merge({'a': {}}, d)
    self.assertEqual(d, {
      'a': {},
    })

    d = {'b': {}}
    partial._merge({'a': {}}, d)
    self.assertEqual(d, {
      'a': {},
      'b': {},
    })

  def test_recursive(self):
    d = {}
    partial._merge({'a': {'b': {}}}, d)
    self.assertEqual(d, {
      'a': {
        'b': {},
      },
    })

    d = {'a': {'c': {}}}
    partial._merge({'a': {'b': {}}}, d)
    self.assertEqual(d, {
      'a': {
        'b': {},
        'c': {},
      },
    })

    d = {'a': {'d': {'e': {}}}}
    partial._merge({'a': {'b': {'c': {}}}}, d)
    self.assertEqual(d, {
      'a': {
        'b': {
          'c': {},
        },
        'd': {
          'e': {},
        }
      },
    })

    d = {'a': {'b': {'c': {'f': {}}, 'g': {}}}}
    partial._merge({'a': {'b': {'c': {'d': {}, 'e': {}}}}}, d)
    self.assertEqual(d, {
      'a': {
        'b': {
          'c': {
            'd': {},
            'e': {},
            'f': {},
          },
          'g': {},
        },
      },
    })


class ParseTestCase(test_case.TestCase):
  """Tests for partial._parse."""

  def test_parse_simple(self):
    self.assertEqual(partial._parse('a'), {
      'a': {},
    })
    self.assertEqual(partial._parse('a,b'), {
      'a': {},
      'b': {},
    })
    self.assertEqual(partial._parse('a,b,c'), {
      'a': {},
      'b': {},
      'c': {},
    })

  def test_parse_components(self):
    self.assertEqual(partial._parse('a/b'), {
      'a': {
        'b': {},
      },
    })
    self.assertEqual(partial._parse('a/b,c'), {
      'a': {
        'b': {},
      },
      'c': {},
    })
    self.assertEqual(partial._parse('a,b/c'), {
      'a': {},
      'b': {
        'c': {},
      },
    })
    self.assertEqual(partial._parse('a/b,c/d'), {
      'a': {
        'b': {},
      },
      'c': {
        'd': {},
      },
    })
    self.assertEqual(partial._parse('a/b/c,d/e/f'), {
      'a': {
        'b': {
          'c': {},
        },
      },
      'd': {
        'e': {
          'f': {},
        },
      },
    })

  def test_parse_subfields(self):
    self.assertEqual(partial._parse('a(b)'), {
      'a': {
        'b': {},
      },
    })
    self.assertEqual(partial._parse('a/b(c)'), {
      'a': {
        'b': {
          'c': {},
        },
      },
    })
    self.assertEqual(partial._parse('a(b/c)'), {
      'a': {
        'b': {
          'c': {},
        },
      },
    })
    self.assertEqual(partial._parse('a(b,c)'), {
      'a': {
        'b': {},
        'c': {},
      },
    })
    self.assertEqual(partial._parse('a/b(c,d)'), {
      'a': {
        'b': {
          'c': {},
          'd': {},
        },
      },
    })
    self.assertEqual(partial._parse('a(b/c,d)'), {
      'a': {
        'b': {
          'c': {},
        },
        'd': {},
      },
    })
    self.assertEqual(partial._parse('a(b,c/d)'), {
      'a': {
        'b': {
        },
        'c': {
          'd': {},
        },
      },
    })
    self.assertEqual(partial._parse('a(b,c(d))'), {
      'a': {
        'b': {},
        'c': {
          'd': {},
         }
      },
    })
    self.assertEqual(partial._parse('a(b),c(d)'), {
      'a': {
        'b': {},
      },
      'c': {
        'd': {},
      },
    })
    self.assertEqual(partial._parse('a(b(c,d),e),f'), {
      'a': {
        'b': {
          'c': {},
          'd': {},
        },
        'e': {},
      },
      'f': {},
    })

  def test_parse_duplicates(self):
    self.assertEqual(partial._parse('a,a'), {
      'a': {},
    })
    self.assertEqual(partial._parse('a,b,a'), {
      'a': {},
      'b': {},
    })
    self.assertEqual(partial._parse('a,b,b,a'), {
      'a': {},
      'b': {},
    })
    self.assertEqual(partial._parse('a/b,a/b'), {
      'a': {
        'b': {},
      },
    })
    self.assertEqual(partial._parse('a/b,a/b,c,d,d'), {
      'a': {
        'b': {},
      },
      'c': {},
      'd': {},
    })
    self.assertEqual(partial._parse('a/b,a(b)'), {
      'a': {
        'b': {},
      },
    })
    self.assertEqual(partial._parse('a(b),a/b,c,d,d'), {
      'a': {
        'b': {},
      },
      'c': {},
      'd': {},
    })
    self.assertEqual(partial._parse('a(b,c),a/b,a/c,a(b),a(c)'), {
      'a': {
        'b': {},
        'c': {},
      },
    })
    self.assertEqual(partial._parse('a/b/c,a(b/d,b/e)'), {
      'a': {
        'b': {
          'c': {},
          'd': {},
          'e': {},
        },
      },
    })

  def test_parse_raises(self):
    fields = [
      '',
      '/',
      'a/',
      '/b',
      'a//b',
      ','
      'a,',
      ',b',
      '/,',
      ',/',
      'a/b,',
      ',a/b',
      '()',
      '(a)',
      '(,)',
      '(a,)',
      '(,b)',
      '(a,b)',
      '(a,b)c',
      'a(())',
      'a((b))',
      'a((b,))',
      'a((,c))',
      'a((b,c))',
      '(',
      'a(',
      'a(b',
      ')',
      'a)',
      'a(b))',
      'a(b),',
      'a(b)c',
      'a(/)',
      'a(b/)',
      'a(/b)',
    ]
    for f in fields:
      with self.assertRaises(partial.ParsingError):
        partial._parse(f)


if __name__ == '__main__':
  if '-v' in sys.argv:
    unittest.TestCase.maxDiff = None
  unittest.main()
