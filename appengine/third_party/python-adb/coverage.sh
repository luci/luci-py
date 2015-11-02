#!/bin/sh
# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

cd "$(dirname $0)"

if [ -e .coverage ]; then rm .coverage; fi

if [ -z ${1+x} ]; then
  FILES=*_test.py
else
  FILES="$@"
fi

for i in $FILES; do
  echo "$i"
  coverage run --append --include "./*.py" "$i"
done
coverage report -m
