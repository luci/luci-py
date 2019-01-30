// Copyright 2019 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

/** taskResult is a helper function for the demo page that parses
 *  the url and returns result data based on the trailing 3 numbers.
 *  This allows the demo page dynamically change based on some
 *  user input, just like the real thing.
 */
export function taskResult(url, opts) {
  let taskId = url.match('/task/(.+)/result');
  taskId = taskId[1] || '000';
  let idx = parseInt(taskId.slice(-3));
  if (!idx) {
    idx = 0;
  }
  return taskResults[idx];
}

export const taskResults = [
  {
    "cipd_pins": {
      "packages": [
        {
          "path": ".",
          "version": "7JNHoA8j-byynAnNNfD93zYxvCrfS_q57UeUhC7oH6YC",
          "package_name": "infra/tools/luci/kitchen/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "b245a31a4df87bd38f7e7d0cf19d492695bd7a7e",
          "package_name": "infra/git/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "46c0c897ca0f053799ee41fd148bb7a47232df47",
          "package_name": "infra/python/cpython/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "J8IGFTojudB9c6rtwsCmlcUA0eCvuf173AdsfeAFe9YC",
          "package_name": "infra/tools/buildbucket/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "_EmLtOFqma-Fdw0ExhHST4uRG3IDfFe8vkba2_1NGZAC",
          "package_name": "infra/tools/cloudtail/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "CCUPRoUSIMjB0H9RYWX4yK7kKAAoCUK864mWSDQdzXIC",
          "package_name": "infra/tools/git/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "cexxITLLto0E5R-VwXpZWQUq1mXCXXGjGbew22M66cMC",
          "package_name": "infra/tools/luci-auth/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "xka0wl1vmSqAJOB7looVmSpSXn_1ztxBtzMc5nN3rqcC",
          "package_name": "infra/tools/luci/docker-credential-luci/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "2FiJ5AgpUA0ardjEakl6gtMPLKwd3X_iQ3HkzFgPNt8C",
          "package_name": "infra/tools/luci/git-credential-luci/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "CxDAdPUaDvFK4dPug3SkX03Cf2Oe2ir67g4I1ZMZ58IC",
          "package_name": "infra/tools/luci/vpython-native/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "uCjugbKg6wMIF6_H_BHECZQdcGRebhnZ6LzSodPHQ7AC",
          "package_name": "infra/tools/luci/vpython/linux-amd64"
        },
        {
          "path": "cipd_bin_packages",
          "version": "qIKuSNcuWDXDxEsV459Y9O38lFmjI0zSFf9fv8bCZ1cC",
          "package_name": "infra/tools/prpc/linux-amd64"
        },
        {
          "path": "kitchen-checkout",
          "version": "KLmG5i5Hnx_RXGGwkowc4S44nF8FXji5guMhHU-pTuMC",
          "package_name": "infra/recipe_bundles/chromium.googlesource.com/chromium/tools/build"
        }
      ],
      "client_package": {
        "version": "yIT5zb0Ieo_5PolHxSBu03UOOGA1iEEXpNISFEoSd-8C",
        "package_name": "infra/tools/cipd/linux-amd64"
      }
    },
    "run_id": "40110b3c0fac7811",
    "outputs_ref": {
      "isolatedserver": "https://isolateserver.appspot.com",
      "namespace": "default-gzip",
      "isolated": "9b7e9a85c14f3012e0395ae1d92d1d7339f5f99e"
    },
    "server_versions": [
      "3779-c5c026e"
    ],
    "performance_stats": {
      "isolated_download": {
        "initial_size": "0",
        "initial_number_items": "0"
      },
      "isolated_upload": {
        "num_items_cold": "2",
        "duration": 0.5382578372955322,
        "total_bytes_items_cold": "12617",
        "items_cold": "eJxrZdyfAAAD+QGm"
      },
      "bot_overhead": 12.625049114227295
    },
    "duration": 881.5171999931335,
    "completed_ts": "2019-01-21T10:42:33.353190",
    "started_ts": "2019-01-21T10:27:38.055897",
    "internal_failure": false,
    "exit_code": "0",
    "state": "COMPLETED",
    "bot_version": "a601d60342c4e8aab332d42ad036f481fab9c080a89f92726c56a2c813228a51",
    "tags": [
      "build_address:luci.chromium.try/linux_chromium_cfi_rel_ng/608",
      "buildbucket_bucket:luci.chromium.try",
      "buildbucket_build_id:8934841822195451424",
      "buildbucket_hostname:cr-buildbucket.appspot.com",
      "buildbucket_template_canary:0",
      "buildbucket_template_revision:1630ff158d8d4118027817e4d74c356b46464ed9",
      "builder:linux_chromium_cfi_rel_ng",
      "buildset:patch/gerrit/chromium-review.googlesource.com/1237113/1",
      "caches:builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2",
      "cores:32",
      "cpu:x86-64",
      "log_location:logdog://logs.chromium.org/chromium/buildbucket/cr-buildbucket.appspot.com/8934841822195451424/+/annotations",
      "luci_project:chromium",
      "os:Ubuntu-14.04",
      "pool:luci.chromium.try",
      "priority:30",
      "source_repo:https://example.com/repo/%s",
      "source_revision:65432abcdef",
      "recipe_name:chromium_trybot",
      "recipe_package:infra/recipe_bundles/chromium.googlesource.com/chromium/tools/build",
      "service_account:chromium-try-builder@example.iam.gserviceaccount.com",
      "swarming.pool.template:skip",
      "swarming.pool.version:a636fa546b9b663cc0d60eefebb84621a4dfa011",
      "user:None",
      "user_agent:git_cl_try",
      "vpython:native-python-wrapper"
    ],
    "failure": false,
    "modified_ts": "2019-01-21T10:42:33.353190",
    "user": "",
    "created_ts": "2019-01-21T10:24:15.851434",
    "name": "Completed task with 2 slices",
    "task_id": "task000",
    "bot_dimensions": [
      {
        "value": [
          "linux_chromium_cfi_rel_ng"
        ],
        "key": "builder"
      },
      {
        "value": [
          "32"
        ],
        "key": "cores"
      },
      {
        "value": [
          "x86",
          "x86-64",
          "x86-64-Haswell_GCE",
          "x86-64-avx2"
        ],
        "key": "cpu"
      },
      {
        "value": [
          "none"
        ],
        "key": "gpu"
      },
      {
        "value": [
          "swarm1931-c4"
        ],
        "key": "id"
      },
      {
        "value": [
          "chrome-trusty-18042300-b7223b463e3"
        ],
        "key": "image"
      },
      {
        "value": [
          "0"
        ],
        "key": "inside_docker"
      },
      {
        "value": [
          "1"
        ],
        "key": "kvm"
      },
      {
        "value": [
          "n1-highmem-32"
        ],
        "key": "machine_type"
      },
      {
        "value": [
          "Linux",
          "Ubuntu",
          "Ubuntu-14.04"
        ],
        "key": "os"
      },
      {
        "value": [
          "luci.chromium.try"
        ],
        "key": "pool"
      },
      {
        "value": [
          "2.7.6"
        ],
        "key": "python"
      },
      {
        "value": [
          "3779-c5c026e"
        ],
        "key": "server_version"
      },
      {
        "value": [
          "us",
          "us-central",
          "us-central1",
          "us-central1-c"
        ],
        "key": "zone"
      }
    ],
    "try_number": "1",
    "current_task_slice": "1",
    "costs_usd": [
      0.5054023369562476
    ],
    "bot_id": "swarm1931-c4"
  }
];


/** taskRequest is a helper function for the demo page that parses
 *  the url and returns request data based on the trailing 3 numbers.
 *  This allows the demo page dynamically change based on some
 *  user input, just like the real thing.
 */
export function taskRequest(url, opts) {
  let taskId = url.match('/task/(.+)/request');
  taskId = taskId[1] || '000';
  let idx = parseInt(taskId.slice(-3));
  if (!idx) {
    idx = 0;
  }
  return taskRequests[idx];
}

export const taskRequests = [
  {
    "created_ts": "2019-01-21T10:24:15.851434",
    "authenticated": "user:iamuser@example.com",
    "name": "Completed task with 2 slices",
    "tags": [
      "build_address:luci.chromium.try/linux_chromium_cfi_rel_ng/608",
      "buildbucket_bucket:luci.chromium.try",
      "buildbucket_build_id:8934841822195451424",
      "buildbucket_hostname:cr-buildbucket.appspot.com",
      "buildbucket_template_canary:0",
      "buildbucket_template_revision:1630ff158d8d4118027817e4d74c356b46464ed9",
      "builder:linux_chromium_cfi_rel_ng",
      "buildset:patch/gerrit/chromium-review.googlesource.com/1237113/1",
      "caches:builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2",
      "cores:32",
      "cpu:x86-64",
      "log_location:logdog://logs.chromium.org/chromium/buildbucket/cr-buildbucket.appspot.com/8934841822195451424/+/annotations",
      "luci_project:chromium",
      "os:Ubuntu-14.04",
      "pool:luci.chromium.try",
      "priority:30",
      "recipe_name:chromium_trybot",
      "recipe_package:infra/recipe_bundles/chromium.googlesource.com/chromium/tools/build",
      "service_account:chromium-try-builder@example.iam.gserviceaccount.com",
      "swarming.pool.template:skip",
      "swarming.pool.version:a636fa546b9b663cc0d60eefebb84621a4dfa011",
      "source_repo:https://example.com/repo/%s",
      "source_revision:65432abcdef",
      "user:None",
      "user_agent:git_cl_try",
      "vpython:native-python-wrapper"
    ],
    "pubsub_topic": "projects/cr-buildbucket/topics/swarming",
    "priority": "30",
    "pubsub_userdata": "{\"build_id\": 8934841822195451424, \"created_ts\": 1537467855732287, \"swarming_hostname\": \"chromium-swarm.appspot.com\"}",
    "user": "",
    "service_account": "chromium-try-builder@example.iam.gserviceaccount.com",
    "task_slices": [
      {
        "expiration_secs": "120",
        "wait_for_capacity": false,
        "properties": {
          "dimensions": [
            {
              "value": "linux_chromium_cfi_rel_ng",
              "key": "builder"
            },
            {
              "value": "32",
              "key": "cores"
            },
            {
              "value": "Ubuntu-14.04",
              "key": "os"
            },
            {
              "value": "x86-64",
              "key": "cpu"
            },
            {
              "value": "luci.chromium.try",
              "key": "pool"
            },
            {
              "value": "builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2",
              "key": "caches"
            }
          ],
          "idempotent": false,
          "outputs": ["first_output", "second_output"],
          "cipd_input": {
            "packages": [
              {
                "path": ".",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/kitchen/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "version:2.17.1.chromium15",
                "package_name": "infra/git/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "version:2.7.14.chromium14",
                "package_name": "infra/python/cpython/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/buildbucket/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/cloudtail/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/git/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci-auth/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:770bd591835116b72af3b6932c8bce3f11c5c6a8",
                "package_name": "infra/tools/luci/docker-credential-luci/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/git-credential-luci/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/vpython-native/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/vpython/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/prpc/${platform}"
              },
              {
                "path": "kitchen-checkout",
                "version": "refs/heads/master",
                "package_name": "infra/recipe_bundles/chromium.googlesource.com/chromium/tools/build"
              }
            ],
            "client_package": {
              "version": "git_revision:fb963f0f43e265a65fb7f1f202e17ea23e947063",
              "package_name": "infra/tools/cipd/${platform}"
            },
            "server": "https://chrome-infra-packages.appspot.com"
          },
          "env_prefixes": [
            {
              "value": [
                "cipd_bin_packages",
                "cipd_bin_packages/bin"
              ],
              "key": "PATH"
            },
            {
              "value": [
                "cache/vpython"
              ],
              "key": "VPYTHON_VIRTUALENV_ROOT"
            }
          ],
          "command": [
            "kitchen${EXECUTABLE_SUFFIX}",
            "cook",
            "-mode",
            "swarming",
            "-build-url",
            "https://ci.chromium.org/p/chromium/builders/luci.chromium.try/linux_chromium_cfi_rel_ng/608",
            "-luci-system-account",
            "system",
            "-repository",
            "",
            "-revision",
            "",
            "-recipe",
            "chromium_trybot",
            "-cache-dir",
            "cache",
            "-checkout-dir",
            "kitchen-checkout",
            "-temp-dir",
            "tmp",
            "-properties",
            "{\"$depot_tools/bot_update\": {\"apply_patch_on_gclient\": true}, \"$kitchen\": {\"devshell\": true, \"git_auth\": true}, \"$recipe_engine/runtime\": {\"is_experimental\": false, \"is_luci\": true}, \"blamelist\": [\"iamuser@example.com\"], \"buildbucket\": {\"build\": {\"bucket\": \"luci.chromium.try\", \"created_by\": \"user:iamuser@example.com\", \"created_ts\": 1537467855227638, \"id\": \"8934841822195451424\", \"project\": \"chromium\", \"tags\": [\"builder:linux_chromium_cfi_rel_ng\", \"buildset:patch/gerrit/chromium-review.googlesource.com/1237113/1\", \"user_agent:git_cl_try\"]}, \"hostname\": \"cr-buildbucket.appspot.com\"}, \"buildername\": \"linux_chromium_cfi_rel_ng\", \"buildnumber\": 608, \"category\": \"git_cl_try\", \"mastername\": \"tryserver.chromium.linux\", \"patch_gerrit_url\": \"https://chromium-review.googlesource.com\", \"patch_issue\": 1237113, \"patch_project\": \"chromium/src\", \"patch_ref\": \"refs/changes/13/1237113/1\", \"patch_repository_url\": \"https://chromium.googlesource.com/chromium/src\", \"patch_set\": 1, \"patch_storage\": \"gerrit\"}",
            "-known-gerrit-host",
            "android.googlesource.com",
            "-known-gerrit-host",
            "boringssl.googlesource.com",
            "-known-gerrit-host",
            "chromium.googlesource.com",
            "-known-gerrit-host",
            "dart.googlesource.com",
            "-known-gerrit-host",
            "fuchsia.googlesource.com",
            "-known-gerrit-host",
            "gn.googlesource.com",
            "-known-gerrit-host",
            "go.googlesource.com",
            "-known-gerrit-host",
            "llvm.googlesource.com",
            "-known-gerrit-host",
            "pdfium.googlesource.com",
            "-known-gerrit-host",
            "skia.googlesource.com",
            "-known-gerrit-host",
            "webrtc.googlesource.com",
            "-logdog-annotation-url",
            "logdog://logs.chromium.org/chromium/buildbucket/cr-buildbucket.appspot.com/8934841822195451424/+/annotations",
            "-output-result-json",
            "${ISOLATED_OUTDIR}/build-run-result.json"
          ],
          "env": [
            {
              "value": "FALSE",
              "key": "BUILDBUCKET_EXPERIMENTAL"
            },
            {
              "value": "/tmp/frobulation",
              "key": "TURBO_ENCAPSULATOR"
            },
          ],
          "execution_timeout_secs": "10800",
          "inputs_ref": {
            "isolatedserver": "https://isolateserver.appspot.com",
            "namespace": "default-gzip"
          },
          "grace_period_secs": "30",
          "caches": [
            {
              "path": "cache/builder",
              "name": "builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2"
            },
            {
              "path": "cache/git",
              "name": "git"
            },
            {
              "path": "cache/goma",
              "name": "goma_v2"
            },
            {
              "path": "cache/vpython",
              "name": "vpython"
            },
            {
              "path": "cache/win_toolchain",
              "name": "win_toolchain"
            }
          ]
        }
      },
      {
        "expiration_secs": "21480",
        "wait_for_capacity": false,
        "properties": {
          "dimensions": [
            {
              "value": "32",
              "key": "cores"
            },
            {
              "value": "linux_chromium_cfi_rel_ng",
              "key": "builder"
            },
            {
              "value": "Ubuntu-14.04",
              "key": "os"
            },
            {
              "value": "x86-64",
              "key": "cpu"
            },
            {
              "value": "luci.chromium.try",
              "key": "pool"
            }
          ],
          "idempotent": false,
          "outputs": ["first_output", "second_output"],
          "cipd_input": {
            "packages": [
              {
                "path": ".",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/kitchen/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "version:2.17.1.chromium15",
                "package_name": "infra/git/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "version:2.7.14.chromium14",
                "package_name": "infra/python/cpython/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/buildbucket/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/cloudtail/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/git/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci-auth/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:770bd591835116b72af3b6932c8bce3f11c5c6a8",
                "package_name": "infra/tools/luci/docker-credential-luci/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/git-credential-luci/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/vpython-native/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/luci/vpython/${platform}"
              },
              {
                "path": "cipd_bin_packages",
                "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
                "package_name": "infra/tools/prpc/${platform}"
              },
              {
                "path": "kitchen-checkout",
                "version": "refs/heads/master",
                "package_name": "infra/recipe_bundles/chromium.googlesource.com/chromium/tools/build"
              }
            ],
            "client_package": {
              "version": "git_revision:fb963f0f43e265a65fb7f1f202e17ea23e947063",
              "package_name": "infra/tools/cipd/${platform}"
            },
            "server": "https://chrome-infra-packages.appspot.com"
          },
          "env_prefixes": [
            {
              "value": [
                "cipd_bin_packages",
                "cipd_bin_packages/bin"
              ],
              "key": "PATH"
            },
            {
              "value": [
                "cache/vpython"
              ],
              "key": "VPYTHON_VIRTUALENV_ROOT"
            }
          ],
          "command": [
            "kitchen${EXECUTABLE_SUFFIX}",
            "cook",
            "-mode",
            "swarming",
            "-build-url",
            "https://ci.chromium.org/p/chromium/builders/luci.chromium.try/linux_chromium_cfi_rel_ng/608",
            "-luci-system-account",
            "system",
            "-repository",
            "",
            "-revision",
            "",
            "-recipe",
            "chromium_trybot",
            "-cache-dir",
            "cache",
            "-checkout-dir",
            "kitchen-checkout",
            "-temp-dir",
            "tmp",
            "-properties",
            "{\"$depot_tools/bot_update\": {\"apply_patch_on_gclient\": true}, \"$kitchen\": {\"devshell\": true, \"git_auth\": true}, \"$recipe_engine/runtime\": {\"is_experimental\": false, \"is_luci\": true}, \"blamelist\": [\"iamuser@example.com\"], \"buildbucket\": {\"build\": {\"bucket\": \"luci.chromium.try\", \"created_by\": \"user:iamuser@example.com\", \"created_ts\": 1537467855227638, \"id\": \"8934841822195451424\", \"project\": \"chromium\", \"tags\": [\"builder:linux_chromium_cfi_rel_ng\", \"buildset:patch/gerrit/chromium-review.googlesource.com/1237113/1\", \"user_agent:git_cl_try\"]}, \"hostname\": \"cr-buildbucket.appspot.com\"}, \"buildername\": \"linux_chromium_cfi_rel_ng\", \"buildnumber\": 608, \"category\": \"git_cl_try\", \"mastername\": \"tryserver.chromium.linux\", \"patch_gerrit_url\": \"https://chromium-review.googlesource.com\", \"patch_issue\": 1237113, \"patch_project\": \"chromium/src\", \"patch_ref\": \"refs/changes/13/1237113/1\", \"patch_repository_url\": \"https://chromium.googlesource.com/chromium/src\", \"patch_set\": 1, \"patch_storage\": \"gerrit\"}",
            "-known-gerrit-host",
            "android.googlesource.com",
            "-known-gerrit-host",
            "boringssl.googlesource.com",
            "-known-gerrit-host",
            "chromium.googlesource.com",
            "-known-gerrit-host",
            "dart.googlesource.com",
            "-known-gerrit-host",
            "fuchsia.googlesource.com",
            "-known-gerrit-host",
            "gn.googlesource.com",
            "-known-gerrit-host",
            "go.googlesource.com",
            "-known-gerrit-host",
            "llvm.googlesource.com",
            "-known-gerrit-host",
            "pdfium.googlesource.com",
            "-known-gerrit-host",
            "skia.googlesource.com",
            "-known-gerrit-host",
            "webrtc.googlesource.com",
            "-logdog-annotation-url",
            "logdog://logs.chromium.org/chromium/buildbucket/cr-buildbucket.appspot.com/8934841822195451424/+/annotations",
            "-output-result-json",
            "${ISOLATED_OUTDIR}/build-run-result.json"
          ],
          "env": [
            {
              "value": "FALSE",
              "key": "BUILDBUCKET_EXPERIMENTAL"
            },
            {
              "value": "/tmp/frobulation",
              "key": "TURBO_ENCAPSULATOR"
            },
          ],
          "execution_timeout_secs": "10800",
          "inputs_ref": {
            "isolatedserver": "https://isolateserver.appspot.com",
            "namespace": "default-gzip"
          },
          "grace_period_secs": "30",
          "caches": [
            {
              "path": "cache/builder",
              "name": "builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2"
            },
            {
              "path": "cache/git",
              "name": "git"
            },
            {
              "path": "cache/goma",
              "name": "goma_v2"
            },
            {
              "path": "cache/vpython",
              "name": "vpython"
            },
            {
              "path": "cache/win_toolchain",
              "name": "win_toolchain"
            }
          ]
        }
      }
    ],
    "expiration_secs": "21600",
    "properties": {
      "dimensions": [
        {
          "value": "linux_chromium_cfi_rel_ng",
          "key": "builder"
        },
        {
          "value": "32",
          "key": "cores"
        },
        {
          "value": "Ubuntu-14.04",
          "key": "os"
        },
        {
          "value": "x86-64",
          "key": "cpu"
        },
        {
          "value": "luci.chromium.try",
          "key": "pool"
        },
        {
          "value": "builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2",
          "key": "caches"
        }
      ],
      "idempotent": false,
      "cipd_input": {
        "packages": [
          {
            "path": ".",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/luci/kitchen/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "version:2.17.1.chromium15",
            "package_name": "infra/git/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "version:2.7.14.chromium14",
            "package_name": "infra/python/cpython/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/buildbucket/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/cloudtail/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/git/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/luci-auth/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:770bd591835116b72af3b6932c8bce3f11c5c6a8",
            "package_name": "infra/tools/luci/docker-credential-luci/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/luci/git-credential-luci/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/luci/vpython-native/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/luci/vpython/${platform}"
          },
          {
            "path": "cipd_bin_packages",
            "version": "git_revision:2c805f1c716f6c5ad2126b27ec88b8585a09481e",
            "package_name": "infra/tools/prpc/${platform}"
          },
          {
            "path": "kitchen-checkout",
            "version": "refs/heads/master",
            "package_name": "infra/recipe_bundles/chromium.googlesource.com/chromium/tools/build"
          }
        ],
        "client_package": {
          "version": "git_revision:fb963f0f43e265a65fb7f1f202e17ea23e947063",
          "package_name": "infra/tools/cipd/${platform}"
        },
        "server": "https://chrome-infra-packages.appspot.com"
      },
      "env_prefixes": [
        {
          "value": [
            "cipd_bin_packages",
            "cipd_bin_packages/bin"
          ],
          "key": "PATH"
        },
        {
          "value": [
            "cache/vpython"
          ],
          "key": "VPYTHON_VIRTUALENV_ROOT"
        }
      ],
      "command": [
        "kitchen${EXECUTABLE_SUFFIX}",
        "cook",
        "-mode",
        "swarming",
        "-build-url",
        "https://ci.chromium.org/p/chromium/builders/luci.chromium.try/linux_chromium_cfi_rel_ng/608",
        "-luci-system-account",
        "system",
        "-repository",
        "",
        "-revision",
        "",
        "-recipe",
        "chromium_trybot",
        "-cache-dir",
        "cache",
        "-checkout-dir",
        "kitchen-checkout",
        "-temp-dir",
        "tmp",
        "-properties",
        "{\"$depot_tools/bot_update\": {\"apply_patch_on_gclient\": true}, \"$kitchen\": {\"devshell\": true, \"git_auth\": true}, \"$recipe_engine/runtime\": {\"is_experimental\": false, \"is_luci\": true}, \"blamelist\": [\"iamuser@example.com\"], \"buildbucket\": {\"build\": {\"bucket\": \"luci.chromium.try\", \"created_by\": \"user:iamuser@example.com\", \"created_ts\": 1537467855227638, \"id\": \"8934841822195451424\", \"project\": \"chromium\", \"tags\": [\"builder:linux_chromium_cfi_rel_ng\", \"buildset:patch/gerrit/chromium-review.googlesource.com/1237113/1\", \"user_agent:git_cl_try\"]}, \"hostname\": \"cr-buildbucket.appspot.com\"}, \"buildername\": \"linux_chromium_cfi_rel_ng\", \"buildnumber\": 608, \"category\": \"git_cl_try\", \"mastername\": \"tryserver.chromium.linux\", \"patch_gerrit_url\": \"https://chromium-review.googlesource.com\", \"patch_issue\": 1237113, \"patch_project\": \"chromium/src\", \"patch_ref\": \"refs/changes/13/1237113/1\", \"patch_repository_url\": \"https://chromium.googlesource.com/chromium/src\", \"patch_set\": 1, \"patch_storage\": \"gerrit\"}",
        "-known-gerrit-host",
        "android.googlesource.com",
        "-known-gerrit-host",
        "boringssl.googlesource.com",
        "-known-gerrit-host",
        "chromium.googlesource.com",
        "-known-gerrit-host",
        "dart.googlesource.com",
        "-known-gerrit-host",
        "fuchsia.googlesource.com",
        "-known-gerrit-host",
        "gn.googlesource.com",
        "-known-gerrit-host",
        "go.googlesource.com",
        "-known-gerrit-host",
        "llvm.googlesource.com",
        "-known-gerrit-host",
        "pdfium.googlesource.com",
        "-known-gerrit-host",
        "skia.googlesource.com",
        "-known-gerrit-host",
        "webrtc.googlesource.com",
        "-logdog-annotation-url",
        "logdog://logs.chromium.org/chromium/buildbucket/cr-buildbucket.appspot.com/8934841822195451424/+/annotations",
        "-output-result-json",
        "${ISOLATED_OUTDIR}/build-run-result.json"
      ],
      "env": [
        {
          "value": "FALSE",
          "key": "BUILDBUCKET_EXPERIMENTAL"
        }
      ],
      "execution_timeout_secs": "10800",
      "inputs_ref": {
        "isolatedserver": "https://isolateserver.appspot.com",
        "namespace": "default-gzip"
      },
      "grace_period_secs": "30",
      "caches": [
        {
          "path": "cache/builder",
          "name": "builder_86e11e72bf6f8c2c424eb2189ffc073b483485cf12a42b403fb5526a59936253_v2"
        },
        {
          "path": "cache/git",
          "name": "git"
        },
        {
          "path": "cache/goma",
          "name": "goma_v2"
        },
        {
          "path": "cache/vpython",
          "name": "vpython"
        },
        {
          "path": "cache/win_toolchain",
          "name": "win_toolchain"
        }
      ]
    }
  }
];

