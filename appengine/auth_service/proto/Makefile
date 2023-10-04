# Copyright 2023 The LUCI Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Files copied from https://chromium.googlesource.com/infra/go/src/go.chromium.org/luci:
#   server/auth/service/protocol/realms.proto
#   server/auth/service/protocol/replication.proto
#   server/auth/service/protocol/security_config.proto
#
# Commit:8f921abc398dd06ecac4bb5a6a5039ee5b191704
# Modifications: see import_luci_go.sh

compile:
	# If we simply compile *.proto files relative to this dir, the recorded file
	# names in the generated _pb2.py are relative to this dir too which may
	# introduce naming collisions in the protobuf registry.
	#
	# Move the proto path to //appengine/components to "namespace" proto files.
	# This is consistent with other components assuming that "components" is in
	# the import path.
	../tools/compile_proto.py --proto_path=../ .
