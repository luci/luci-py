# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Set up the local directory to run the demo pages.

default:
	bower install

# Run a local HTTP server for the demo pages.
.PHONY: run
run:
	npm install http-server
	# We use sinon to mock up the demo page's json requests.
	npm install sinon@1.17.2
	node_modules/.bin/http-server -p 8080 -a 127.0.0.1