# IM 消息链路可观测性

## 目标

当用户反馈“发送成功但对方没有及时收到”时，按消息经过的真实边界定位，而不是只检查 HTTP 是否返回成功：

```text
HTTP → PostgreSQL/message+outbox → Outbox Worker → Redis 路由
→ Hub → Client Channel → WebSocket → Sync API → 客户端 ACK
```

单条消息使用结构化日志中的 `message_id` 和 `event_id` 关联。Prometheus 指标只做聚合，不使用用户、设备、Session、连接、消息或事件 ID 作为标签。

## 暴露方式

指标默认监听在 `127.0.0.1:9090/metrics`，通过 `METRICS_ADDR` 修改。这个监听器应只暴露在内网或监控网络，不能直接开放到公网。

应用使用独立 Prometheus Registry，避免测试和多实例初始化共享全局注册状态。数据库聚合指标在抓取时执行两条只读聚合查询，并设置一秒超时；建议 Prometheus 抓取间隔从 15～30 秒开始，再根据数据库成本调整。

## 关键指标

| 指标 | 含义 | 主要定位层 |
| --- | --- | --- |
| `im_backend_http_requests_total` | 稳定路由、方法和状态码维度的请求数 | HTTP/API |
| `im_backend_http_request_duration_seconds` | 稳定路由的请求耗时 | HTTP/API、数据库 |
| `im_backend_outbox_pending_events` | 尚未发布且未进入 dead 的事件数 | Outbox |
| `im_backend_outbox_oldest_pending_age_seconds` | 最老待发布事件年龄 | Outbox |
| `im_backend_outbox_dead_events` | 已进入 dead 的事件数 | Outbox、人工处理 |
| `im_backend_outbox_publish_total` | published、retry、dead、lease_lost、state_error 等结果 | Outbox Publisher |
| `im_backend_outbox_publish_duration_seconds` | 单事件纯 Publisher 耗时，不含数据库状态收尾 | Hub / Redis Publisher |
| `im_backend_outbox_stage_duration_seconds` | 非空批次的 claim、prepare、publish、mark_published 关键路径耗时；prepare 另分 decode、begin、project_users、encode、store、commit 子阶段 | Outbox Worker |
| `im_backend_outbox_worker_concurrency` / `batch_size` | 当前 Worker 并发槽位与单次 claim 批量 | Outbox 配置 |
| `im_backend_outbox_pipeline_enabled` | `0` 为整批串行执行，`1` 为 prepare 与上一批 deliver 重叠 | Outbox 配置 |
| `im_backend_outbox_projection_bulk_enabled` | `0` 为逐用户 SQL，`1` 为批量投影实验实现 | Outbox 配置 |
| `im_backend_outbox_projection_recipients_enabled` | `0` 为 JSONB payload 回写，`1` 为结构化 recipients + ready | Outbox 配置 |
| `im_backend_outbox_projection_batches_total` / `users_total` | 投影批次数与每批涉及的唯一用户总数；二者相除得到平均用户数/批 | Sync 投影 |
| `im_backend_outbox_projection_query_duration_seconds` | `project_users` 内单次 SQL 的客户端观测耗时；`count / batches` 得到 SQL 次数/批 | PostgreSQL 往返、执行与结果读取 |
| `im_backend_realtime_routing_total` | 本地 Hub、Presence、Redis 发布/订阅各阶段结果 | Redis/跨实例路由 |
| `im_backend_websocket_connections` | 当前实例 Hub 中的连接数 | Hub |
| `im_backend_websocket_deliveries_total` | queued、no_connection、slow_client | Channel/慢连接 |
| `im_backend_websocket_disconnects_total` | 撤销、替换、慢连接、过期、关闭等有限原因 | WebSocket 生命周期 |
| `im_backend_websocket_io_total` | WebSocket 写消息和 Ping 的成功/失败数 | WebSocket/网络 |
| `im_backend_sync_pages_total` | Sync API 返回页数及是否还有后续页 | 断线恢复 |
| `im_backend_sync_events_total` | Sync API 返回的事件数 | 断线恢复 |
| `im_backend_ack_requests_total` | accepted、ahead、invalid、error | 客户端 ACK |
| `im_backend_device_sync_max_ack_lag` | 已记录设备中最大的同步流与 ACK 差值 | 客户端落盘/补拉 |
| `im_backend_database_metrics_collection_success` | 数据库聚合指标是否抓取成功 | 监控自身 |
| `im_backend_database_pool_*` | 应用连接池上限、占用/空闲数、空池等待次数与累计等待时间 | PostgreSQL 连接池 |

## 排障顺序

1. 用 HTTP 状态码和耗时确认请求是否进入并完成服务端持久化。
2. 检查 Outbox 待处理数和最老事件年龄；持续增长说明异步发布跟不上。
3. 用 `outbox_stage_duration_seconds` 区分 claim、Sync 投影、实时 publish 和状态收尾。`outbox_pipeline_enabled=0` 时四阶段串行相加；为 `1` 时分别比较准备 Lane（claim + prepare）和投递 Lane（publish + mark），不能再把四项之和当成批次完成间隔。任一 Lane 超过到达预算，pending age 都会持续增长。
4. 若 `prepare_project_users` 最大，用 projection 的 batches、users 和 query count 判断是否仍在按用户执行 SQL；该 Histogram 包含服务端执行、锁等待、数据库往返和结果读取，不能单独解释为纯网络 RTT。
5. 若 `prepare_store` 最大，先看 `outbox_projection_recipients_enabled`：JSONB 模式表示 payload 回写成本，recipients 模式表示结构化 recipient 插入与 ready 更新成本，二者不能混为同一种写入。
6. 检查 `outbox_publish_total` 的 retry、dead、lease_lost 和 state_error。
7. 检查 Presence 查询和 Redis publish/receive 的 error、no_subscriber。
8. 检查 Hub 是否没有连接，以及 slow_client 是否增加。
9. 检查 WebSocket write/ping 错误；实时链路允许失败，客户端随后应走 Sync。
10. 检查 Sync 返回量和 ACK lag；实时层正常但 ACK 落后通常表示客户端没有完成落盘或补拉。
11. 单条事件使用日志中的 `message_id`、`event_id` 查询对应 Outbox 状态和失败记录。

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
```

阈值必须通过真实压测和一段时间的基线数据确定；上述 `30 秒` 只是便于首次验证的示例，不是已经确认的生产 SLO。

## 当前边界

- 已有指标、聚合采集器、独立指标监听器和结构化关联日志。
- 尚未部署 Prometheus、Grafana Dashboard 或 Alertmanager 规则。
- 尚未接入分布式 Trace，因此目前单条消息主要依赖 `message_id`、`event_id` 和数据库状态关联。
- ACK lag 包含所有仍保留状态的设备；正式告警前还需要定义“活跃设备”时间窗口和设备状态清理策略。
