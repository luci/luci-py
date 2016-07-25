# Copyright 2016 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

# Set up the local directory to run the demo pages.

default:
	bower install

# Run a local HTTP server for the demo pages.
.PHONY: run
run: default
	node_modules/.bin/http-server -p 8080 -a 127.0.0.1


.PHONY: vulcanize
vulcanize:
	./node_modules/.bin/vulcanize --inline-css=true --inline-scripts=true --strip-comments=true --out-html=./build/index-build.html index.html
	./node_modules/.bin/vulcanize --inline-css=true --inline-scripts=true --strip-comments=true --out-html=./build/botlist-build.html botlist.html

.PHONY: clean_vulcanize
clean_vulcanize: default
	rm -f ./build/*build.html
	./node_modules/.bin/vulcanize --inline-css=true --inline-scripts=true --strip-comments=true --out-html=./build/index-build.html index.html
	./node_modules/.bin/vulcanize --inline-css=true --inline-scripts=true --strip-comments=true --out-html=./build/botlist-build.html botlist.html