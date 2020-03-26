# `isolate` user guide

Isolate your test.


## Introduction

-   The Go binary `isolate` (compiled from
    https://chromium.googlesource.com/infra/luci/luci-go/+/refs/heads/master/client/cmd/isolate/main.go)
    is meant to replace `isolate.py`.
-   `isolate` wraps the vast majority of the client side code relating to
    executable isolation.
-   "`isolate help`" gives you all the help you need so only a quick overview is
    given here.


## isolate

-   "`isolate`" wraps usage for tracing, compiling, archiving and even running a
    test isolated locally.
-   Look at the `isolate help` page for more information.
-   `-isolate` is the preferred input format in the Go
    implementation. `-isolated` is supported in only a few subcommands (e.g.
    `isolate archive`).

### Minimal .isolate file

Here is an example of a minimal .isolate file where an additional file is needed
only on Windows and the command there is different:
```
{
  1. Global level.
  'variables': {
    'files': [
      '<(PRODUCT_DIR)/foo_unittests<(EXECUTABLE_SUFFIX)',
      1. All files in a subdirectory will be included.
      '../test/data/',
    ],
  },

  1. Things that are configuration or OS specific.
  'conditions': [
    ['OS=="linux" or OS=="mac"', {
      'variables': {
        'command': [
          '<(PRODUCT_DIR)/foo_unittests<(EXECUTABLE_SUFFIX)',
        ],
      },
    }],

    ['OS=="android"', {
      'variables': {
        'command': [
          'setup_env.py',
          '<(PRODUCT_DIR)/foo_unittests<(EXECUTABLE_SUFFIX)',
        ],
        'files': [
          'setup_env.py',
        ],
      },
    }],
  ],
}
```


The `EXECUTABLE_SUFFIX` variable is automatically set to `".exe"` on Windows and
empty on the other OSes.

Working on Chromium? You are not done yet! You need to create a GYP target too,
check out http://dev.chromium.org/developers/testing/isolated-testing/for-swes


### Useful subcommands

-   "`isolate check`" verifies a `.isolate` file (It no longer produces a
    `.isolated`).
-   "`isolate archive`" does the equivalent of `check`, then archives the
    isolated tree.
-   "`isolate run`" runs the test locally isolated, so you can verify for any
    failure specific to isolated testing.

Did I tell you about "`isolate help`" yet?


## FAQ

### I have a feature request / This looks awesome, can I contribute?

This project is 100% open source. See
[Contributing](https://github.com/luci/luci-py/wiki/Contributing) page for more
information.
