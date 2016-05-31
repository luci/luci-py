# Running a Swarming Bot

## Requirements

*   All platforms
    *   `python` is in `PATH`.
    *   HTTPS access to *.appspot.com and Cloud Storage (GCS).
    *   Unique bot id across the fleet. The bot id uses the base hostname (not
        FQDN) by default so by default, hostnames should be unique.
*   Android
    *   On an Debian desktop host, the current user must be a member
        of `plugdev` so it can open USB ports.
    *   python [libusb1](https://pypi.python.org/pypi/libusb1) is installed.
*   GNU/linux and OSX
    *   Must have passwordless `sudo reboot` enabled, otherwise the bot will
        hang when trying to reboot the host.

        Add the following line in `/etc/sudoers`
        where `swarming` is the account in which the swarming bot runs in:

            swarming ALL=NOPASSWD:/sbin/shutdown -r now

*   iOS
    *   Xcode must be installed and the EULA accepted. It can be automated with:

            sudo xcodebuild -license accept

    *   Install (compile) [libmobiledevice](http://www.libimobiledevice.org/)
        and both `ideviceinfo` and `idevice_id` must be in `PATH`.


## Recommendataions

*   Username in which the bot runs is `swarming`. It is recommend to not run the
    bot as root!
*   `swarming` account autologin is enabled so the bots can start within a UI
    context, important on on OSX and Windows. It is less important on GNU/linux
    based distributions since generally Xvfb can be used for UI testing. This
    can be done in [bot_config.py](../swarming_bot/config/bot_config.py)
    `setup_bot()` function leveraging
    [os_utilities.py](../swarming_bot/api/os_utilities.py):
    *   OSX: `setup_auto_startup_osx()`
    *   Ubuntu/Debian:
        `setup_auto_startup_autostart_desktop_linux()` if gnome based UI is used
        with autologin, otherwise `setup_auto_startup_initd_linux()` but this
        requires root access, which is trickier.
    *   Windows: `setup_auto_startup_win()`
*   Android
    *   Android devices should be rooted so efficient power management can be
        used. This can greatly improve throughput by accelerating cooling down
        during downtimes.
*   OSX
    *   Enable dtrace and powermetrics if desired for testing purpose. Warning:
        enabling dtrace effectively gives control over the host. Add the
        following to `/etc/sudoers`:

            swarming ALL=NOPASSWD:/usr/sbin/dtrace
            swarming ALL=NOPASSWD:/usr/bin/powermetrics
*   Testing involving hardware audio requires a 3.5mm dongle to be plugged in
    the audio port.


### Auto-starting on Debian

#### Automatic start via rc.local

That's the cheezy way for a one-off quick hack.

Do the following modification to /etc/rc.local:

    --- /etc/rc.local
    +++ /etc/rc.local
    @@ -17,4 +17,6 @@
      printf "My IP address is %s\n" "$_IP"
    fi

    +sudo -i -u swarming python /path/to/swarming_bot.zip start_bot &
    +
    exit 0


#### Automatic start via systemd

For systemd enabled GNU/linux distributions, e.g. Ubuntu 16.04, Debian Jessie.
This is much cleaner because systemd takes care of starting up the process in
time, automatic restart on crash, logging, etc.

As root (sudo -i) run the following:

    cat > /etc/systemd/system/swarming_bot.service << EOF
    [Unit]
    Description=Swarming bot
    After=network.target

    [Service]
    Type=simple
    User=swarming
    Restart=always
    RestartSec=10
    ExecStart=/usr/bin/env python /path/to/swarming_bot.zip start_bot

    [Install]
    WantedBy=multi-user.target
    EOF

    systemctl daemon-reload
    systemctl enable swarming_bot.service
    systemctl start swarming_bot.service
    systemctl status


## Bot sizing

How to calculate the CPU/RAM/Disk space for each Swarming bot.

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


### Compiler toolset

In general, swarming bots will not do compilation so only the runtime
dependencies to run tests should be installed. Note that not installing the
compiler may have side-effects that are known to break the tests, for example on
Windows, tests requires installing both the Release and Debug CRTs if they are
built with the corresponding CRT DLLs. This needs to be investigated on an
OS-per-OS and each test at a time.


### Swarming Bot configuration

#### Disk space usage

`run_isolated.py` by default keeps a local cache of 20gb or 100,000 items, which
ever comes first. It also enforces that at least 2gb of free disk space remains
at the start and end of execution. Taking in account the swarm_bot code is of
negligible size and the amount of data used in the chrome infrastructure is
fairly static, 30gb of disk space should be sufficient for Chrome purposes.
YMMV. This could have to be revisited in case of significant use case change,
e.g. starting to run tests for another project. As an example, Blink layout
tests consist of ~80k files so it is near the 100k items default limit. As such,
50gb would be ample free room, anything above is likely wasted space.

Because the swarming bots do not compile and always run the tests in a
temporary directory, deleting it afterward, using 2 separate partitions is not
as useful as for normal try bots.


#### CPU and RAM Specing

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
