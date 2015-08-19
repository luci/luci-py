# Copyright 2015 The Swarming Authors. All rights reserved.
# Use of this source code is governed by the Apache v2.0 license that can be
# found in the LICENSE file.

test: build
	./run_coverage.py

build: compile-proto

compile-proto:
	cd proto && make
