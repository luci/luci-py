# Source this file from within python-adb.
#
# Adds the following packages from luci-py:
# - python-libusb1
# - python-rsa
# - pyasn1

THIS_DIR="$(pwd)"
export PYTHONPATH="$THIS_DIR/../python-libusb1:$THIS_DIR/../../../client/third_party/rsa:$THIS_DIR/../../../client/third_party/pyasn1:$PYTHONPATH"
