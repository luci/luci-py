# Copyright 2017 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Allow multiline strings in text protobuf format.

Usage:

  text_proto = multiline_proto.parse(multiline_text_proto)
  protobuf.text_format.Merge(text_proto, msg)

`parse` looks for bash-style heredocs and replaces them with single-line
text-proto-escaped strings.

Example:
  this: <<EOF
    would
      turn \ninto
      a "single"
    line
  EOF

Turns into the following:
  this: "would\nturn \\ninto\n  a \"single\"\nline"

The format must be compatible with
https://github.com/luci/luci-go/blob/master/common/proto/multiline.go

In particular, the inner lines will be treated with `textwrap.dedent`;
any common leading whitespace that occurs on every line will be
removed. Although both tabs and spaces count as whitespace, they are not
equivalent (i.e. only exactly-matching whitespace prefixes count).
"""

from .multiline_proto import parse, ParseError
