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
   * @param {String} taskId - id of task to cancel.
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
}
