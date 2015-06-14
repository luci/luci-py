# Bot sizing

How to calculate the CPU/RAM/Disk space for each Swarming bot.

## Summary

The swarming infrastructure is an external parallelization infrastructure. It
takes multiple steps traditionally run serially and runs them concurrently on
separate hosts. This is an horizontal trade-off between high performance and
wide horizontal scalability to reduce end-to-end latency.

The external parallelization is done via isolated testing. Some steps are not
externally parallelizable, like compilation. The Swarming infrastructure is not
adapted to parallelize compilation externally, due to the various latencies
involved.

The end result is that the Swarming bots do not need to be high performance.
High performance VMs should be kept for bots doing internal parallelized steps,
like compilation which requires a source tree checkout. In contrast, it's better
to have more slower Swarming bots to reduce the overall latency.


## Compiler toolset

In general, swarming bots will not do compilation so only the runtime
dependencies to run tests should be installed. Note that not installing the
compiler may have side-effects that are known to break the tests, for example on
Windows, tests requires installing both the Release and Debug CRTs if they are
built with the corresponding CRT DLLs. This needs to be investigated on an
OS-per-OS and each test at a time.


## Swarming Bot configuration

### Disk space usage

`run_isolated.py` by default keeps a local cache of 20gb or 100,000 items, which
ever comes first. It also enforces that at least 2gb of free disk space remains
at the start and end of execution. Taking in account the swarm_bot code is of
negligible size and the amount of data used in the chrome infrastructure is
fairly static, 30gb of disk space should be sufficient for Chrome purposes.
YMMV. This could have to be revisited in case of significant use case change,
e.g. starting to run tests for another project. As an example, Blink layout
tests consist of ~80k files so it is near the 100k items default limit. As such,
50gb would be ample free room, anything above is likely wasted space.

Because the swarming bots do not compile and always runs the tests in a
temporary directory, deleting it afterward, using 2 separate partitions is not
as useful as for normal try bots.


### CPU and RAM Specing

First and foremost it is important to make the tests use as much CPU as
possible, in particular if scaling is near linear with the number of CPUs on the
machine. The ratio of RAM vs CPU is purely dependent on the tests that are going
to be run. In general 1~2GB per CPU is fine. Remember these VMs do not build, so
memory usage is relatively low compared to a machine that compiles and links.

On the other hand, the constant cost of setup, fetching the files, and tear down
are either network I/O or disk I/O bound and use a relatively small amount of
RAM and CPU. So the constant cost doesn't change much with the increase of RAM
or CPU, leading to a higher relative cost for this constant cost when comparing
1 VM with 8 cores vs 2 VMs with 4 cores.
