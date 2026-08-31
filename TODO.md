# TODO

## Sync/Outbox recipient 统一 A/B

- [x] 增加 `OUTBOX_PROJECTION_STORAGE=sync_events` 候选：正常准备事务只写 `user_message_events` 并标记 Outbox ready，已 ready 事件在重试时按 `message_id` 从 Sync 行重建 user/cursor。
- [x] 用真实 PostgreSQL 验证提交后崩溃恢复、Lease 丢失回滚、自发自收，以及 `recipients ↔ sync_events` 双向切换恢复。
- [x] 在 3500 req/s 稳定负载上按 `recipients A1 → sync_events B1 → sync_events B2 → recipients A2` 使用四个新鲜隔离库运行；每轮重启 PostgreSQL/Redis，四轮 HTTP、Realtime、Sync 都完整。
- [x] 直接目标指标方向一致：`prepare_store` 两轮中点从 `4.83ms` 降到 `1.81ms`（-62.4%），准备 Lane 从 `13.71ms` 降到 `11.14ms`（-18.7%）；每轮 140000 条重复 recipient 写入降为 0。Realtime/pending 峰值未同步改善，因此不宣称容量提升。
- [x] 将 `sync_events` 切为默认，保留 `OUTBOX_PROJECTION_STORAGE=recipients` 回退开关。全量测试、race、vet 通过；不显式设置存储开关的 1000 条真实链路 smoke 完成 HTTP 1000/1000、Realtime 4000/4000、Sync 2000/2000，且 `outbox_recipients=0`、pending/dead 为 0。
- [ ] 切换默认后先保留 `outbox_recipients` 回退窗口；只有确认没有旧版 Worker 且已 ready 事件全部排空后，才用独立迁移删表。

## 下一容量升档：区分 Pool 等待与事务执行

- [x] 恢复 3000 req/s 完整基线，并确认该档共享 Pool 没有形成显著连接等待。

  2026-08-31 的新鲜隔离库复核完成 60000/60000 HTTP、240000/240000 Realtime、120000/120000 Sync，dropped、missing、duplicate、unexpected、dead 均为 0。API/Outbox acquisition P95 分别不高于 `0.10ms/0.01ms`，因此不在 3000 档直接做分池。

  实验前置：

  - [x] 用 `im_backend_database_pool_acquire_duration_seconds{workload="api|outbox",result="success|error"}` 分别记录两侧 acquisition 耗时；压测报告的 `databaseAcquireDurations` 保存测试前后差值。未标记的启动探活和监控查询不混入这两组。
  - [x] 保留事务内各 SQL 阶段计时，并把 acquisition 与拿到连接后的阶段墙钟时间分开观察。共享 Pool 的整体指标继续保留。
  - 排除宿主机残留计算任务，并在每轮前用同一健康检查确认 PostgreSQL、Redis、API 和指标端点就绪。

  下一步：

  - [x] 按用户要求跳过 3200，使用新鲜隔离状态运行一轮 3500 req/s；70000/70000 HTTP、280000/280000 Realtime、140000/140000 Sync 完整，dropped/missing/dead 为 0。
  - [x] 使用新鲜隔离状态运行一轮 4000 req/s；80000/80000 HTTP、320000/320000 Realtime、160000/160000 Sync 完整，但 API/Outbox acquisition P95 已升到 `25/1ms`，准备 Lane 约 `15.30ms`，接近 `16ms` 预算。
  - [x] 使用新鲜隔离状态运行 5000 req/s；仅 97664/100000 HTTP 成功，Realtime/Sync 未在核验窗口内完整，pending/oldest 峰值达到 `83072/36.738s`，首次明确越界；最终数据库计数完整且 pending/dead 为 0，不是持久化丢失。
  - [ ] 在 4000～5000 区间内使用新鲜状态复测，找到第一个可重复失败档；单轮 4000 通过不能当作稳定容量上限。
  - [ ] 同时比较 API/Outbox acquisition、`prepare_store`、`prepare_project_users`、HTTP、Realtime 和 pending/oldest，不能从一个平均阶段直接猜原因。

  5000 已出现 API/Outbox acquisition P95 `250/50ms`；在区间内找到可重复失败档后，执行分池 A/B：

  - A：API 与 Worker 共用 Pool，总连接数固定为 24。
  - B：API 与 Worker 分池，但总连接数仍固定为 24；初始候选为 API 18 + Worker 6，后续只根据 acquisition 指标校准。
  - 两组使用新鲜隔离库、相同 Redis 状态、相同 Batch/并发/消息分布，按 `A1 → B1 → B2 → A2` 运行；若 acquisition 没有放大，则跳过分池，改为针对实测最慢 SQL 阶段设计 A/B。

  正确性和结束标准：

  - HTTP 必须完成全部目标消息，dropped starts、failed、Realtime/Sync missing、duplicate、unexpected、dead 均为 0；否则只能记为诊断轮。
  - 先在 3000 req/s 复现完整基线，再逐档升压；比较连接等待、事务执行、HTTP P95/P99、Realtime P95/P99、pending/oldest 峰值。
  - 若分池只转移饥饿而没有提高完整吞吐，不采用；随后再单独 A/B `prepare_store` 的重复 user/cursor 写入，不能把两个改动混在一轮。

## 下一个容量实验：多不同用户的 Presence 批内缓存

- [ ] 扩展压测器，验证收件人基数增大时，批内 Presence 去重从“高命中”到“低命中”的性能边界。

  当前压测器把 `-users` 限制在 2～10，并且要求 `users * devices <= 30`，因为所有用户都会通过同一测试机 IP 登录并建立 WebSocket。不能直接把上限改成 1000，否则首先测到的会是登录限流和建连成本，而不是 Presence 缓存。

  实验前置：

  - 将“消息参与者数”与“在线 WebSocket 用户数”拆成独立参数，保留有界的登录和连接数。
  - 让发送分布可配置，能够稳定产生 10、64、128 个不同收件人/批；Batch 64 且每事件两个 recipient 时，单批理论上限是 128。
  - 为大量离线收件人保留 Sync durability 核验，不能因为没有全部建立 WebSocket 就删除正确性验证。

  A/B 矩阵：

  - A：`OUTBOX_BATCH_PRESENCE_LOOKUP=false`，每个 recipient 独立查 Presence。
  - B：`OUTBOX_BATCH_PRESENCE_LOOKUP=true`，同批用户去重后用两段 Redis pipeline 生成短命快照。
  - 每个用户基数都用新鲜隔离库，固定 Pool、Batch、Worker 并发、pipeline 模式、请求速率和消息数，并使用 `A1 → B1 → B2 → A2` 顺序平衡。

  必须记录：

  - `outbox_batch_presence_users_total / outbox_batch_presence_batches_total`，即实际唯一用户数/批。
  - Presence 解析次数、`publish_prepare`、整批 `publish`、Realtime P95/P99、pending/oldest 峰值、HTTP P95/P99 与 Redis 错误。
  - HTTP、Realtime 和 Sync 的 missing、duplicate、unexpected、dead；任一正确性项失败，该轮不能用来宣称容量改善。

  结束标准：说清在多少唯一用户/批时，批内缓存仍有净收益，以及收益消失后的下一个实测瓶颈。
