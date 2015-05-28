# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

"""Handlers for HTTP requests."""

import handlers_backend
import handlers_endpoints


endpoints_app = handlers_endpoints.create_endpoints_app()
backend_app = handlers_backend.create_backend_app()
