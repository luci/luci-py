# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

components_ereporter2_RECIPIENTS_AUTH_GROUP = 'config-ereporter2-reports'

from components import utils
utils.import_third_party()
utils.fix_protobuf_package()
