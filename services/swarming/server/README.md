# Task scheduler

The task scheduling core has to be read in order like a story in 4 parts; each
block depends on the previous ones:

* task_request.py
* task_shard_to_run.py
* task_result.py
* task_scheduler.py


## General workflow

* A client wants to run something (a task) on the infrastructure, the
  TaskRequest describing this request is saved to note that a new request
  exists. The details of the task is saved in TaskProperties embedded in
  TaskRequest.
* TaskShardToRun entities are created to describe each shard for this request.
  If a TaskRequest.properties.number_shards = 3, there will be 3 TaskShardToRun
  instances.
* Each TaskShardToRun is marked as ready to be triggered. A TaskResultSummary is
  created to describe the request's overall status.
* Bots poll for work. Once a bot reaps a TaskShardToRun, it creates then updates
  the corresponding TaskShardResult and updates it as required. TaskShardResult
  describes the shard specific result, while TaskResultSummary is the sum (and
  summary) of all TaskShardResult.
* Everytime a TaskShardResult is updated, a task queue is enqueued to update the
  corresponding TaskResultSummary.


## Overall schema graph of a task request with 2 shards

       +------Root------+
       |TaskRequestShard|                                     task_request.py
       +----------------+
               ^
               |
    +---------------------+
    |TaskRequest          |
    |    +--------------+ |<..........................+       task_request.py
    |    |TaskProperties| |                           .
    |    +--------------+ |<..+                       .
    +---------------------+   .                       .
     ^                        .                       .
     | +--------Root-------+  . +--------Root-------+ .
     | |TaskShardToRunShard|  . |TaskShardToRunShard| .  task_shard_to_run.py
     | +-------------------+  . +-------------------+ .
     |             ^          .          ^            .
     |             |          .          |            .
     |      +--------------+  .   +--------------+    .
     |      |TaskShardToRun|..+   |TaskShardToRun|....+  task_shard_to_run.py
     |      +--------------+      +--------------+
     |                     ^                 ^
     |                     |                 |
    +-----------------+    +---------------+ +---------------+
    |TaskResultSummary|    |TaskShardResult| |TaskShardResult|    task_result.py
    +-----------------+    +---------------+ +---------------+
            .                       ^           ^
            .                       .           .
            +.......................+...........+

### Notes

* Each root entity is tagged as Root.
* Each line is annoted with the file that define the entities on this line.
* Dotted line means a similar key relationship without actual entity hierarchy.
