Swarming
========

An AppEngine service to do task scheduling on highly hetegeneous fleets at
medium (10000s) scale. It is focused to survive network with low reliability,
high latency while still having low bot maintenance, and no server maintenance
at all since it's running on AppEngine.


Documentation
-------------

Design doc:
https://github.com/luci/luci-py/wiki/Swarming-Design

Reference:
https://github.com/luci/luci-py/wiki/Swarming-User-Guide

Technical details about the implementation can be found in
[server/README.md](server).
