# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

build: compile-proto

compile-proto:
	cd components/config/proto && make

test: build
	tools/run_coverage.py

deploy: build
	tools/gae upload -x

upload: build
	tools/gae upload

rollback:
	appcfg.py -V `tools/gae version` rollback .
