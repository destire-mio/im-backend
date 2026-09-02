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
| `im_backend_outbox_stage_duration_seconds` | 非空批次的 claim、publish_prepare（批内 Presence）、publish、mark_published 耗时；publish 包含 publish_prepare，不能简单相加 | Outbox Worker |
| `im_backend_outbox_worker_concurrency` / `batch_size` | 当前 Worker 并发槽位与单次 claim 批量 | Outbox 配置 |
| `im_backend_outbox_batch_presence_batches_total` / `users_total` | Presence 快照批次与其唯一用户总数；二者相除得到平均唯一用户数/批 | Redis Presence |
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
4. 对比 claim、publish_prepare、publish 和 mark_published。新消息已经在发送事务中 ready，不再有 SQL 投影阶段；批内 Presence 预取计入 publish_prepare，也包含在 publish 总耗时里。
5. 检查 outbox_publish_total 的 retry_scheduled、dead、lease_lost 和 state_error。
6. 检查平均唯一用户数/批，再检查 Presence 查询与 Redis publish/receive 的 error、no_subscriber。
7. 检查 Hub 是否没有连接，以及 slow_client 是否增加。
8. 检查 WebSocket write/ping 错误；实时链路允许失败，客户端随后应走 Sync。
9. 检查逐会话 Sync 返回量和 ACK lag；实时层正常但某个会话 ACK 落后通常表示客户端没有完成该会话的落盘或补拉。
10. 单条事件使用日志中的 message_id、event_id 查询对应 Outbox 状态和失败记录。

迁移 016 后不再暴露投影模式、投影任务或算法开关指标；历史 Dashboard 若使用这些指标，需要删除对应查询。旧 v1/v2/v3 的 pending 事件由迁移一次性转换，published/dead 旧事件只是归档记录，不是另一个运行时模式。

## PromQL 示例

```promql
# Outbox 最老事件超过 30 秒
im_backend_outbox_oldest_pending_age_seconds > 30

# 五分钟内 Outbox 重试速率
sum(rate(im_backend_outbox_publish_total{result="retry_scheduled"}[5m])) by (event_type)

# Outbox 关键阶段的 P95
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
