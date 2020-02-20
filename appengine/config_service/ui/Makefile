# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

.PHONY: default release build test install clean_install clean


release:
	make clean_install
	npx polymer build
	npm prune --production

build: install
	npx polymer build

test: install
	npx polymer test

install: package.json
	npm install

clean_install: package-lock.json
	npm ci

clean:
	rm -rf node_modules build