# IM 消息链路可观测性

## 目标

当用户反馈“发送成功但对方没有及时收到”时，按消息经过的真实边界定位，而不是只检查 HTTP 是否返回成功：

```text
HTTP → PostgreSQL/message+outbox → Outbox Worker → Redis 路由
→ Hub → Client Channel → WebSocket → 会话列表/逐会话 Sync → 按会话 ACK
```

单条消息使用结构化日志中的 `message_id` 和 `event_id` 关联。Prometheus 指标只做聚合，不使用用户、设备、Session、连接、消息或事件 ID 作为标签。

## 暴露方式

指标默认监听在 `127.0.0.1:9090/metrics`，通过 `METRICS_ADDR` 修改。这个监听器应只暴露在内网或监控网络，不能直接开放到公网。

应用使用独立 Prometheus Registry，避免测试和多实例初始化共享全局注册状态。数据库聚合指标在抓取时执行三条只读聚合查询，并设置一秒超时；建议 Prometheus 抓取间隔从 15～30 秒开始，再根据数据库成本调整。

## 关键指标

| 指标 | 含义 | 主要定位层 |
| --- | --- | --- |
| `im_backend_http_requests_total` | 稳定路由、方法和状态码维度的请求数 | HTTP/API |
| `im_backend_http_request_duration_seconds` | 稳定路由的请求耗时 | HTTP/API、数据库 |
| `im_backend_outbox_pending_events` | 尚未发布且未进入 dead 的事件数 | Outbox |
| `im_backend_outbox_oldest_pending_age_seconds` | 最老待发布事件年龄 | Outbox |
| `im_backend_outbox_dead_events` | 已进入 dead 的事件数 | Outbox、人工处理 |
| `im_backend_outbox_publish_total` | published、retry、dead、lease_lost、state_error 等结果 | Outbox Publisher |
| `im_backend_outbox_publish_duration_seconds` | 单事件 Publisher 耗时，不含批内 Presence 预取和数据库状态收尾 | Hub / Redis Publisher |
| `im_backend_outbox_stage_duration_seconds` | 非空批次的 claim、prepare、publish、mark_published 关键路径耗时；inline prepare 另分 decode、begin、project_users、encode、store、commit；用户分片模式另分 projection_dispatch、projection_begin、projection_claim、projection_project_users、projection_store、projection_commit、projection_batch | Outbox Worker |
| `im_backend_outbox_worker_concurrency` / `batch_size` | 当前 Worker 并发槽位与单次 claim 批量 | Outbox 配置 |
| `im_backend_outbox_prepare_workers` / `user_sharded_prepare_enabled` | 迁移前 v3 Sync 投影的物理 Worker 数，以及是否启用按用户分片模式；v4 不使用 | Outbox 兼容配置 |
| `im_backend_outbox_projection_pending_jobs` | 尚未完成的 v3 用户投影临时任务数；v4 正常路径应保持为 0 | 旧事件排空 |
| `im_backend_outbox_projection_oldest_pending_job_age_seconds` | 最老未完成 v3 用户投影任务的年龄 | 旧事件排空 |
| `im_backend_outbox_pipeline_enabled` | `0` 为整批串行执行，`1` 为 prepare 与上一批 deliver 重叠 | Outbox 配置 |
| `im_backend_outbox_batch_presence_enabled` | `0` 为每 recipient 独立查 Presence，`1` 为同批用户去重并批量预取 | Outbox 配置 |
| `im_backend_outbox_batch_presence_batches_total` / `users_total` | Presence 快照批次与其唯一用户总数；二者相除得到平均唯一用户数/批 | Redis Presence |
| `im_backend_outbox_projection_bulk_enabled` | v3 兼容投影使用逐用户或批量 SQL；不描述 v4 正常写入 | Outbox 兼容配置 |
| `im_backend_outbox_projection_recipients_enabled` | v3 回退到结构化 `outbox_recipients` + ready | Outbox 兼容配置 |
| `im_backend_outbox_projection_sync_events_enabled` | v3 复用 `user_message_events` 恢复投递对象 | Outbox 兼容配置 |
| `im_backend_outbox_projection_batches_total` / `users_total` | v3 投影批次数与唯一用户总数；v4 正常路径不应增长 | 旧 Sync 投影 |
| `im_backend_outbox_projection_query_duration_seconds` | v3 `project_users` SQL 的客户端观测耗时 | 旧 Sync 投影 |
| `im_backend_realtime_routing_total` | 本地 Hub、Presence、Redis 发布/订阅各阶段结果 | Redis/跨实例路由 |
| `im_backend_websocket_connections` | 当前实例 Hub 中的连接数 | Hub |
| `im_backend_websocket_deliveries_total` | queued、no_connection、slow_client | Channel/慢连接 |
| `im_backend_websocket_disconnects_total` | 撤销、替换、慢连接、过期、关闭等有限原因 | WebSocket 生命周期 |
| `im_backend_websocket_io_total` | WebSocket 写消息和 Ping 的成功/失败数 | WebSocket/网络 |
| `im_backend_sync_pages_total` | 逐会话消息 Sync API 返回页数及是否还有后续页 | 断线恢复 |
| `im_backend_sync_events_total` | 逐会话 Sync 返回的消息数 | 断线恢复 |
| `im_backend_ack_requests_total` | accepted、ahead、not_found、invalid、gone、error | 客户端 ACK |
| `im_backend_device_sync_max_ack_lag` | 已记录设备/会话状态中最大的 `conversation.last_seq - applied_seq` | 客户端落盘/补拉 |
| `im_backend_database_metrics_collection_success` | 数据库聚合指标是否抓取成功 | 监控自身 |
| `im_backend_database_pool_acquire_duration_seconds` | `workload=api/outbox`、`result=success/error` 维度的连接获取耗时；默认两者共享 Pool，启用隔离后分别来自两个 Pool | PostgreSQL 连接池争用 |
| `im_backend_database_pool_*` | API Pool 上限、占用/空闲数、空池等待次数与累计等待时间 | PostgreSQL API 连接池 |
| `im_backend_outbox_database_pool_*` | 仅在 `OUTBOX_DATABASE_MAX_CONNECTIONS>0` 时出现，描述独立 Outbox Pool | PostgreSQL Outbox 连接池 |

## 排障顺序

1. 用 HTTP 状态码和耗时确认请求是否进入并完成服务端持久化。
2. 对比 `database_pool_acquire_duration_seconds` 的 API 与 Outbox P95。默认共享 Pool 时，两者同时升高且 Pool 空池等待增长说明正在争用；启用独立 Outbox Pool 时，再分别对照 `database_pool_*` 与 `outbox_database_pool_*`。这个 Histogram 测的是调用方取得连接前的总等待，不是 SQL 执行时间。
3. 检查 Outbox 待处理数和最老事件年龄；持续增长说明异步发布跟不上。
4. 先确认待处理事件版本。v4 在发送事务内已经 ready，inline `prepare` 只做兼容分流且不写用户投影；若 v4 的 prepare 明显变慢，应先查 claim/调度而不是 `user_sync_counters`。只有 v1～v3 旧事件才继续看 `user_sharded_prepare_enabled` 与 `projection_*`。
5. 排空 v3 时，同时比较 Outbox pending 与 projection pending：两者一起增长且 projection oldest 增长，说明旧消息卡在用户 Sync 投影；projection 已清零而 Outbox 仍增长，才继续看 ready 之后的 claim/publish/mark。正常全 v4 稳态下 projection pending 应为 0。
6. 若 `prepare_project_users` 或 `projection_project_users` 最大，用 projection 的 batches、users 和 query count 判断 SQL 次数与用户数；该 Histogram 包含服务端执行、锁等待、数据库往返和结果读取，不能单独解释为纯网络 RTT。
7. 排空 v3 时，若 `prepare_store` 或 `projection_store` 最大，先看 projection storage gauge；`sync_events=1` 时旧 user/cursor 只在 `user_message_events`，用户分片的 store 还包括任务完成、Outbox ready 门控和临时任务清理。这一判断不适用于 v4 正常写入。
8. 检查 `outbox_publish_total` 的 retry、dead、lease_lost 和 state_error。
9. 检查 `batch_presence_enabled`、`publish_prepare` 和平均唯一用户数/批，再检查 Presence 查询与 Redis publish/receive 的 error、no_subscriber。
10. 检查 Hub 是否没有连接，以及 slow_client 是否增加。
11. 检查 WebSocket write/ping 错误；实时链路允许失败，客户端随后应走 Sync。
12. 检查逐会话 Sync 返回量和 ACK lag；实时层正常但某个会话 ACK 落后通常表示客户端没有完成该会话的落盘或补拉。
13. 单条事件使用日志中的 `message_id`、`event_id` 查询对应 Outbox 状态和失败记录。

## PromQL 示例

```promql
# Outbox 最老事件超过 30 秒
im_backend_outbox_oldest_pending_age_seconds > 30

# 五分钟内 Outbox 重试速率
sum(rate(im_backend_outbox_publish_total{result="retry_scheduled"}[5m])) by (event_type)

# Outbox 关键阶段和 prepare 子阶段的 P95
histogram_quantile(
  0.95,
  sum(rate(im_backend_outbox_stage_duration_seconds_bucket[5m])) by (le, stage)
)

# Redis 跨实例路由错误
sum(rate(im_backend_realtime_routing_total{result="error"}[5m])) by (stage)

# 慢连接断开速率
rate(im_backend_websocket_disconnects_total{reason="slow_client"}[5m])

# HTTP 路由 P95 耗时
histogram_quantile(
  0.95,
  sum(rate(im_backend_http_request_duration_seconds_bucket[5m])) by (le, route, method)
)

# API 与 Outbox 获取数据库连接的 P95
histogram_quantile(
  0.95,
  sum(rate(im_backend_database_pool_acquire_duration_seconds_bucket{result="success"}[5m])) by (le, workload)
)
```

阈值必须通过真实压测和一段时间的基线数据确定；上述 `30 秒` 只是便于首次验证的示例，不是已经确认的生产 SLO。

## 当前边界

- 已有指标、聚合采集器、独立指标监听器和结构化关联日志。
- 尚未部署 Prometheus、Grafana Dashboard 或 Alertmanager 规则。
- 尚未接入分布式 Trace，因此目前单条消息主要依赖 `message_id`、`event_id` 和数据库状态关联。
- ACK lag 包含所有仍保留的设备/会话状态；正式告警前还需要定义“活跃设备”和“活跃会话”时间窗口及状态清理策略。
