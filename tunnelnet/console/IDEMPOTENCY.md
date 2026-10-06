# 写请求幂等与重放防护

所有会改变状态的 `POST` 请求必须携带客户端生成的请求标识：

- 请求头：`Idempotency-Key: <key>`（也兼容 `X-Request-Id`）
- 或 JSON 字段：`idempotency_key`

## 语义

1. 服务端在执行业务动作前，把请求标识、方法、路径、参数指纹落盘为 `pending`，随后标记 `running`。
2. 同一标识的并发重复请求按同一把文件锁串行；业务执行完成前到达的重复请求等待，不会再次进入业务处理器。
3. 已完成的请求重放时不执行业务动作，只返回落盘的原始状态码和响应体，并带 `Idempotency-Replay: true`。
4. 同一标识绑定完整请求指纹；相同标识但方法、路径、查询或业务 JSON 不同，返回 `409 Conflict`。
5. 业务失败同样保存失败结果；重复请求返回上次失败，不自动重试。人工补发必须生成新的请求标识。
6. 进程在 `pending/running` 阶段重启后，记录变为 `interrupted`；终态结果落盘失败时变为 `uncertain`。两类请求不会自动重放业务，需人工核对后调用恢复接口。
7. 过期只把已结算记录标记为 `expired`，不删除记录或结果；`interrupted/uncertain` 以及尚未结算的请求受保护。过期标识重放返回 `410 Gone`，也不能复用执行。

## 运维接口

- `GET /api/idempotency/keys?request_id=<key>`：查看幂等记录。
- `POST /api/idempotency/recover`：人工结算 `interrupted/uncertain` 记录；`outcome=failed` 时固化失败结果，`outcome=succeeded` 时固化人工确认的成功结果。
- `POST /api/idempotency/expire`：将超过 TTL 的已结算记录标记为过期。

幂等记录、执行结果、生命周期审计和业务审计事件使用同一个请求标识；业务审计记录包含 `request_id` 字段，重启后随幂等记录重新装入审计流水。
