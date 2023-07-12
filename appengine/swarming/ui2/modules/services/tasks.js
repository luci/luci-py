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
   * @returns {Object} with shape {canceled, was_running} - see CancelResponse in https://source.chromium.org/chromium/infra/infra/+/main:luci/appengine/swarming/proto/api_v2/swarming.proto for more details.
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
   * @returns {Object} see https://source.chromium.org/chromium/infra/infra/+/main:luci/appengine/swarming/proto/api_v2/swarming.proto;l=719?q=TaskOutputResponse
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
   * @returns {Object} see https://source.chromium.org/chromium/infra/infra/+/main:luci/appengine/swarming/proto/api_v2/swarming.proto;l=618?q=TaskRequestResponse&sq= to view the return type proto.
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
}
