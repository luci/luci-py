#!/bin/bash
# Copyright 2020 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

cd $(dirname $0)

if [ $# != 1 ]; then
    echo "usage: $ $0 <version>"
    echo "e.g. $ $0 1.24.1"
    exit 1
fi

version="${1}"

find . | fgrep '/' | fgrep -v './update.sh' | fgrep -v 'README.swarming' | \
    fgrep -v './post_handshake_auth.patch' | sort -r | xargs rm -r
curl -sL https://github.com/urllib3/urllib3/archive/${version}.tar.gz | \
    tar xvz --strip-components 3 urllib3-${version}/src/urllib3

patch -p4 < ./post_handshake_auth.patch
