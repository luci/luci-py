# proto

Contains protobuf definitions:

  * [api_v2/](api_v2/): public pRPC API.
  * [plugin/](plugin/): API used by Quota Scheduler (deprecated).
  * [config/](config/): Swarming LUCI Config schemas.
  * [internals/](internals/): internal APIs used by Swarming itself.
  * [api/](api/): deprecated APIs, **to be deleted soon**.

These protos are also exported into luci-go.git [here]. Up-to-date BigQuery
schemas are defined in luci-go.git.

[here]: https://chromium.googlesource.com/infra/luci/luci-go/+/refs/heads/main/swarming/proto/
