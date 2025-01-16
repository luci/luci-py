# Copyright 2025 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
"""String Pairs Serializer for TaskProperties.

To be used to calculate the same properties_hash as the Go implementation.
"""

import struct


class StringPair(object):

  def __init__(self, key, value):
    self.key = key
    self.value = value


class StringPairsSerializer(object):

  def __init__(self):
    self.current = ""
    self.pairs = []

  def enter(self, field):
    if not field:
      raise ValueError("field cannot be empty")
    if "." in field:
      raise ValueError("field cannot contain a dot: %r" % field)
    if not self.current:
      self.current = field
      return
    self.current = self.current + "." + field

  def exit(self):
    if not self.current:
      return
    parts = self.current.rsplit(".", 1)
    if len(parts) == 1:
      self.current = ""
      return
    self.current = parts[0]

  def write_string(self, sub_key, val):
    key = sub_key
    if self.current:
      key = self.current + "." + sub_key
    if not val:
      val = ""
    self.pairs.append(StringPair(key, val))

  def write_bool(self, key, b):
    if b:
      self.write_string(key, "true")
    else:
      self.write_string(key, "false")

  def write_int(self, key, i):
    if i is None:
      i = 0
    self.write_string(key, str(i))

  def write_string_slice(self, key, string_slice, should_sort):
    self.enter(key)
    if should_sort:
      string_slice.sort()
    for i, val in enumerate(string_slice):
      self.write_string(str(i), val)
    self.exit()

  def write_env(self, env):
    if not env:
      return
    self.enter("env")
    keys = sorted(env.keys())
    for i, k in enumerate(keys):
      self.enter(str(i))
      self.write_string("key", k)
      self.write_string("value", env[k])
      self.exit()
    self.exit()

  def write_env_prefixes(self, prefixes):
    if not prefixes:
      return
    self.enter("env_prefixes")
    keys = sorted(prefixes.keys())
    for i, k in enumerate(keys):
      self.enter(str(i))
      self.write_string("key", k)
      self.write_string_slice("value", prefixes[k], False)
      self.exit()
    self.exit()

  def write_task_dimensions(self, dimensions):
    if not dimensions:
      return
    self.enter("dimensions")
    keys = sorted(dimensions.keys())
    for i, k in enumerate(keys):
      self.enter(str(i))
      self.write_string("key", k)
      self.write_string_slice("value", dimensions[k], True)
      self.exit()
    self.exit()

  def write_cache_entries(self, caches):
    self.enter("caches")
    # Sort caches by name then by path
    caches.sort(key=lambda c: (c.name, c.path))
    for i, cache in enumerate(caches):
      self.enter(str(i))
      self.write_string("name", cache.name)
      self.write_string("path", cache.path)
      self.exit()
    self.exit()

  def write_cas_reference(self, cas):
    if not cas:
      return
    self.enter("cas_input_root")
    self.write_string("cas_instance", cas.cas_instance)
    self.write_cas_digest(cas.digest)
    self.exit()

  def write_cas_digest(self, digest):
    if not digest:
      return
    self.enter("digest")
    self.write_string("hash", digest.hash)
    self.write_int("size_bytes", digest.size_bytes)
    self.exit()

  def write_cipd_input(self, cipd):
    if not cipd:
      return
    self.enter("cipd_input")
    self.write_string("server", cipd.server)
    self.write_cipd_package("client_package", cipd.client_package)
    self.write_cipd_packages("packages", cipd.packages)
    self.exit()

  def write_cipd_package(self, key, pkg):
    if not pkg:
      return
    self.enter(key)
    self.write_string("package_name", pkg.package_name)
    self.write_string("version", pkg.version)
    self.write_string("path", pkg.path)
    self.exit()

  def write_cipd_packages(self, key, packages):
    self.enter(key)
    packages.sort(key=lambda x: (x.package_name, x.version, x.path))
    for i, pkg in enumerate(packages):
      self.write_cipd_package(str(i), pkg)
    self.exit()

  def write_containment(self, containment):
    if not containment:
      return
    self.enter("containment")
    self.write_int("containment_type", containment.containment_type)
    self.write_bool("lower_priority", containment.lower_priority)
    self.write_int("limit_processes", containment.limit_processes)
    self.write_int("limit_total_committed_memory",
                   containment.limit_total_committed_memory)
    self.exit()

  def write_task_properties(self, props):
    if not props:
      return

    # Handle basic types and struct fields
    self.write_bool("idempotent", props.idempotent)
    self.write_string("relativeCwd", props.relative_cwd)
    self.write_int("execution_timeout_secs", props.execution_timeout_secs)
    self.write_int("grace_period_secs", props.grace_period_secs)
    self.write_int("io_timeout_secs", props.io_timeout_secs)

    # Handle slices (command, outputs)
    self.write_string_slice("command", props.command, False)
    self.write_string_slice("outputs", props.outputs, True)

    # Handle maps (env, env_prefixes, dimensions)
    self.write_env(props.env)
    self.write_env_prefixes(props.env_prefixes)
    self.write_task_dimensions(props.dimensions)

    # Handle nested structs
    self.write_cache_entries(props.caches)
    self.write_cas_reference(props.cas_input_root)
    self.write_cipd_input(props.cipd_input)
    self.write_containment(props.containment)

  def to_bytes(self, props, secret_bytes):
    self.write_task_properties(props)
    if secret_bytes:
      self.write_string("secret_bytes", secret_bytes.secret_bytes)

    buf = []
    for pair in self.pairs:
      buf.append(struct.pack("<i", len(pair.key)))
      buf.append(pair.key.encode('utf-8'))
      buf.append(struct.pack("<i", len(pair.value)))
      buf.append(pair.value.encode('utf-8'))
    return "".join(buf)
