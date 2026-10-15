# Copyright 2014 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# Bootstraps a swarming bot to connect host_url. 'host_url = "foo"' and
# 'bootstrap_token = "<urlsafe string>"' are added on top of this file by the
# HTTP handler that serves this file. This file must be concise since it is
# executed directly at the CLI.
import os
import urllib
import sys

urllib.urlretrieve(
  "%s/bot_code?tok=%s" % (host_url, bootstrap_token),  # noqa: F821
  "swarming_bot.zip",  # noqa: F821
)
os.execv(sys.executable, [sys.executable, "swarming_bot.zip"])
