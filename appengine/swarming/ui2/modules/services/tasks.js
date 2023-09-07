// Copyright 2023 The LUCI Authors. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

import { PrpcService } from "./common";

export class TasksService extends PrpcService {
  get service() {
    return "swarming.v2.Tasks";
  }

  /**
   * Cancels task for given taskId
   *
   * @param {string} taskId - id of task to cancel.
   * @param {boolean} killRunning - whether to kill task while running.
   *
   * @returns {Object} with shape {canceled, was_running} - see CancelResponse in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#702
   **/
  cancel(taskId, killRunning) {
    return this._call("CancelTask", {
      task_id: taskId,
      kill_running: killRunning,
    });
  }

  /**
   * Gets the standard output of the task.
   *
   * @param {string} taskId - id of task to retrieve.
   * @param {number} offset - number of bytes from begining of task output to start.
   * @param {number} length - number of bytes to retrieve.
   *
   * @returns {Object} TaskOutputResponse object described in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#720
   **/
  stdout(taskId, offset, length) {
    return this._call("GetStdout", {
      task_id: taskId,
      offset: offset,
      length: length,
    });
  }

  /**
   * Retrieves task_request for given taskId.
   *
   * @param {string} taskId - id of task request to retrieve.
   *
   * @returns {Object} - TaskRequest object described in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#618
   **/
  request(taskId) {
    return this._call("GetRequest", {
      task_id: taskId,
    });
  }

  /**
   * Retrieves task_result for givenTaskId.
   *
   * @param {string} taskId - id of task to retrieve
   * @param {boolean} includePerformanceStats - whether to include performance stats with the taskId
   *
   * @returns {Object} see - https://crsrc.org/i/luci/appengine/swarming/proto/api_v2/swarming.proto;l=738?q=TaskResultResponse&sq=
   **/
  result(taskId, includePerformanceStats) {
    return this._call("GetResult", {
      task_id: taskId,
      include_performance_stats: includePerformanceStats,
    });
  }

  /**
   * Creates a new task with the given newTask specification.
   *
   * @param {Object} NewTask specification described in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#526
   *
   * @returns {Object} returns a TaskRequestMetadataResponse described in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#923
   **/
  new(newTaskSpec) {
    return this._call("NewTask", newTaskSpec);
  }

  /**
   * Counts tasks from a given start date filtered by tags and state.
   *
   * @param {Array} tags is an of strings with the form ["k1:v1", "k2:v2", ....]
   * @param {Date} start is the time from which to beging counting tasks.
   * @param {string} state is a string representing the task state. Allowed values are found in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#127
   *
   * @returns {Object} TasksCount object described in https://chromium.googlesource.com/infra/luci/luci-py/+/ba4f94742a3ce94c49432417fbbe3bf1ef9a1fa0/appengine/swarming/proto/api_v2/swarming.proto#917
   **/
  count(tags, start, state) {
    return this._call("CountTasks", { tags, start, state });
  }
}
