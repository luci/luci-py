#!/bin/bash
# Copyright 2019 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

# This is currently depending on wheels outside of the Chrome infrastructure.
# This will have to be converted to a vpython3 configuration (?) but for the
# immediate needs this is good enough to enable us to make progress in porting
# components/.

set -eu

cd "`dirname $0`"

if [ ! -d ./venv3 ]; then
  echo "Creating ./venv3"
  python3 -m venv venv3
fi

echo "Activating venv3"
source ./venv3/bin/activate

echo "Installing requirements.txt"
pip3 install -r requirements.txt

echo "Don't forget to run:"
echo "  source ./venv3/bin/activate"
