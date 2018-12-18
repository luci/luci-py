# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

build: compile-proto

compile-proto:
	cd components/config/proto && make

test: build
	tools/run_coverage.py

deploy_dev: build
	tools/gae upload -x -A luci-config-dev

upload_dev: build
	tools/gae upload -A luci-config-dev

deploy_staging: build
	tools/gae upload -x -A luci-config-staging

upload_staging: build
	tools/gae upload -A luci-config-staging

deploy: build
	tools/gae upload -x -A luci-config

upload: build
	tools/gae upload -A luci-config

