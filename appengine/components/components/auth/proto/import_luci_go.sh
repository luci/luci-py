#!/bin/bash
# Copyright 2023 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# Copies some protos from luci-go, assuming infra.git gclient
# layout. Generates Makefile and _pb2.py.

set -e

MYPATH=$(dirname "${BASH_SOURCE[0]}")
cd ${MYPATH}

LUCI_GO=../../../../../../go/src/go.chromium.org/luci
LUCI_GO_PROTOS_DIR=${LUCI_GO}/server/auth/service/protocol/components/auth/proto

# Kill all existing files.
rm -f realms* replication* security_config*

# Copy fresh files.
cp \
  ${LUCI_GO_PROTOS_DIR}/realms.proto \
  ${LUCI_GO_PROTOS_DIR}/replication.proto \
  ${LUCI_GO_PROTOS_DIR}/security_config.proto \
  \
  .

# Make proto import paths relative to the new root.
sed -i '' 's|import "go.chromium.org/luci/server/auth/service/protocol/components/auth/proto/|import "components/auth/proto/|g' ./*.proto

# Put the revision of copied files into generate.go for posterity.
luci_go_rev=$(git -C ${LUCI_GO} rev-parse HEAD)

sed -i "" "s|# Commit:.*|# Commit:${luci_go_rev}|g" ./Makefile


# Generate *.pb2.py.
Make
