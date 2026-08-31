# TODO

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
  - [ ] 重复 4000 或继续升档，直到出现第一个可重复的正确性、延迟或积压边界；单轮通过不能当作稳定容量上限。
  - [ ] 同时比较 API/Outbox acquisition、`prepare_store`、`prepare_project_users`、HTTP、Realtime 和 pending/oldest，不能从一个平均阶段直接猜原因。

  4000 单轮 acquisition 已明显放大；若重复轮方向一致，执行分池 A/B：

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
