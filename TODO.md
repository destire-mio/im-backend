# TODO

## 当前阻塞实验：PostgreSQL Pool 争用分离 A/B

- [ ] 先恢复可重复的 3000 req/s 完整基线，再判断 Presence 优化后的新容量上限。

  最新的 3200 和三轮 3000 对照都出现 dropped starts，不能作为容量结果；但四轮都显示 `publish` 保持在 `1.80～2.73ms`，HTTP、`prepare_begin`、`claim`、`mark_published` 与 PostgreSQL 空池等待同时放大。当前需要先分清“拿不到连接”和“拿到连接后 SQL 慢”各占多少。

  实验前置：

  - 为 API 和 Outbox 分别记录 Pool acquisition wait；事务内各 SQL 阶段继续单独计时，避免把连接等待误记成 SQL 执行时间。
  - 记录 API/Worker 各自的 acquired、idle、empty acquire、累计等待和事务耗时；压测报告必须保存这些差值。
  - 排除宿主机残留计算任务，并在每轮前用同一健康检查确认 PostgreSQL、Redis、API 和指标端点就绪。

  A/B 矩阵：

  - A：API 与 Worker 共用 Pool，总连接数固定为 24。
  - B：API 与 Worker 分池，但总连接数仍固定为 24；初始候选为 API 18 + Worker 6，后续只根据 acquisition 指标校准。
  - 两组使用新鲜隔离库、相同 Redis 状态、相同 Batch/并发/消息分布，按 `A1 → B1 → B2 → A2` 运行。

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
