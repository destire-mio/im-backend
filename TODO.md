# TODO

## 当前维护状态（016 contract，尚未部署到现有数据库）

- [x] 认证限流改为所有维度检查通过后原子计数；真实 Redis 回归覆盖局部洪泛不耗尽全局额度、全局拒绝不扣局部额度、并发上限及窗口过期。
- [x] 发送事务直接提交 ready v4 Outbox；删除运行时用户级 Sync 投影、分片 Dispatcher/Worker、三种投影存储模式与串行/Presence 回退开关。
- [x] 新增 016 停写迁移：补齐回滚期间缺失的会话 cursor，转换 pending v1/v2/v3 并保留事件身份和退避状态，收紧约束，删除五张旧表与旧索引。001～015 不改 checksum。
- [x] 隔离 PostgreSQL 已验证旧积压转换、自发消息、终态记录保留、异常数据整体回滚、重复迁移，以及迁移链与 schema.sql 的结构一致；保留 015 基线接管仅用于升级，不支持旧二进制运行。
- [x] 用原 015 schema 创建独立升级副本，写入 v1/v2/v3 混合 pending 积压，执行 016 后启动真实服务：3/3 事件均以 v4 ready/published 完成，dead=0，会话序号为 1～3。此项验证无在线接收者时的 Worker 发布完成；在线实时与会话补拉由下项 smoke 覆盖。
- [x] 本轮隔离 PostgreSQL / Redis 的全仓 `go test -race -count=1 ./...`、`go vet ./...`、`go build ./...` 和格式检查通过。真实服务进程 smoke：HTTP 100/100、Realtime 400/400、会话 Sync 200/200，missing/duplicate/unexpected/pending/dead 均为 0；数据库核对 100 条消息对应 100 条 ready/published v4 事件。这是功能验收，不是容量结论。
- [ ] 现有环境的备份、停服、执行 016、部署新二进制：本轮未执行，需要单独安排；迁移后不支持直接回退旧二进制。
- [ ] 纯 v4 当前实现的容量阶梯需要重测，历史报告不能替代。

旧 /messages/sync 和 /messages/ack 仅保留 410 提示，不读写旧同步数据。

> 以下是改造前的历史实验记录，不代表当前生产入口、容量结论或待办；重现实验请使用对应 Git 历史版本。

## 历史记录：Conversation-scoped Sync 实验

- [x] 增加 `conversations`、`conversation_members`、`messages.conversation_id` 与会话内连续 `conversation_seq`；单聊使用规范化用户对保证双向消息落在同一会话。
- [x] `POST /messages` 在一个事务内完成会话解析、序号分配、消息写入和 ready 的 v4 Outbox；并发幂等冲突回滚临时序号，不留下 cursor 空洞。
- [x] 增加会话列表快照分页、逐会话消息快照分页和按设备/会话单调 ACK；旧 `/messages/sync` 与 `/messages/ack` 返回 `410 Gone`，避免静默漏掉 v4 消息。
- [x] WebSocket v4 同时携带 `conversationId` / `conversationSeq`；inline 和 user-sharded 兼容投影都跳过 v4，v3 仍可排空。
- [x] `015` 已在旧 schema 副本上真实验证：双向消息回填到同一会话、序号连续、自发自收只生成一个成员、未发布 v1/v2/v3 payload 补齐会话 cursor。
- [x] 新 Worker 已在该迁移副本上真实排空 v1/v2/v3 混合 backlog：三种版本全部 ready/published、dead=0，v3 仍正确补齐旧用户 Sync 行。
- [x] 独立库真实链路 smoke 完成 100/100 HTTP、200/200 Realtime、200/200 会话 Sync，missing/duplicate/unexpected 为 0；这只证明链路正确，不是容量结论。
- [x] 使用新鲜隔离数据库/Redis 建立首轮 v4 容量阶梯：`ring` 2500 req/s 通过、3000 失败；`hot` 1000 通过、2000 失败。所有已提交消息的 Realtime/Sync 正确性完整，失败点由 dropped starts、连接池等待和 Outbox 峰值判定。
- [x] 增加双进程真实边界测试：跨 Redis Pub/Sub 投递、双实例 200 条并发突发，以及一个实例被杀后通过会话 Sync 恢复。
- [x] 针对 3000 `ring` 与 2000 `hot` 拆分发送事务和 Pool 贡献：既有会话跳过冲突写；序号、消息和 ready Outbox 合并为一条 SQL。`ring` 3000 从 54552/60000 恢复到 60000/60000，HTTP P95 从 409.77ms 降到 16.73ms。
- [x] 完成独立 Outbox Pool 诊断轮：`hot` 2000 的 Realtime P95 从 17.48s 降到 1.13s、pending 峰值从 29987 降到 843，但 HTTP 只完成 32735/40000；`ring` 3000 HTTP P95 回升到 119.78ms。结论是隔离能保护实时链路但不是写入扩容，默认保持共享；`OUTBOX_DATABASE_MAX_CONNECTIONS>0` 仅作为显式隔离开关。
- [ ] 单会话 `hot` 2000 仍受严格连续 `conversation_seq` 串行化限制；任何序号分段或放宽顺序方案都需要先确认产品语义，不能当成本轮机械清理继续修改。
- [ ] 补做“热点会话 vs 高基数分散会话”受控 A/B：固定目标 QPS、消息数、Pool、Outbox 和正确性验证，对比 1 个热点会话与约 5 万个分散会话。高基数路径必须绕过会话 get-or-create，或单独记录 `conversation_resolve`，以便真正隔离 `conversation_seq` 行竞争；不再把“10 万用户各发 1 条”当成独立的全局 QPS 结论。
- [ ] 生产数据量较大时，把当前停写、单事务回填的 `015` 拆成 expand → 在线分批 backfill → 校验 → enforce constraints；当前迁移只适合可接受维护窗口的规模。
- [ ] 确认所有 v1～v3 Outbox 已 published/dead、没有旧 Worker 且回退窗口结束后，再用独立 contract migration 删除用户级 Sync/projector 表、旧路由和 014 方向索引。

> 下列 projection、recipient 与容量实验记录属于 v3 用户级 cursor 架构。实现和开关现已删除；以下勾选项与未完成实验均归档，不再是当前维护清单。

## Prepare Worker 按用户分片候选

- [x] 将每条 `message.created` 拆成每个参与用户一项的临时持久化投影任务；自发自收只生成一项。
- [x] 固定 256 个逻辑 shard，使用 `shard % worker_count` 分配给物理 Worker；首个实验配置为 4 个 Worker，调整物理数量不重写任务 shard。
- [x] 每个逻辑 shard 使用 PostgreSQL advisory transaction lock 保证跨实例单所有者；同一用户的 `user_sync_counters` 仍严格串行，避免 cursor 重复或跳号。
- [x] 只有参与者任务全部完成才在同一事务中标记 Outbox ready；ready 后删除临时任务，避免按每条消息永久保留两行。
- [x] 真实 PostgreSQL 已验证单侧完成不可发布、半完成恢复、自发自收、4 Worker + 10 用户并发，以及每用户连续 cursor。
- [x] 用新鲜隔离数据库和空 Redis DB 运行 5000 req/s 首轮诊断：HTTP 仅 82173/100000，projection pending/oldest 峰值 `115450/46.373s`；停流后 82173 条消息最终对应 164346 条连续 Sync 事件，jobs/pending/dead 为 0。
- [x] 修复 ready 门控把 `event_id` 列转成 text 导致的全索引扫描：改成输入 ID 转 UUID。82173 行表上的同形执行计划由扫描 82173 项、38890 个 shared buffer、92.078ms，变为 64 次 UUID 主键点查、239 个 buffer、0.237ms；大表上的投影集成测试通过。
- [x] UUID 修复后的同规格轮次把 `projection_store` 从 193.56ms 降到 15.59ms、projection jobs 峰值从 115450 降到 36480，成功写入消息的 Realtime/Sync 全部完整；UUID 全扫描已不再是首要问题。
- [x] 确认修复轮的串行 Dispatcher 满批上限约为 3053 messages/s，4 个投影 Worker 满批上限约为 7960 jobs/s，均低于 5000 条双人消息所需的速率。
- [x] 启动 4 个 Dispatcher，每个每次取 64 条，保持总批量窗口仍为 256。首轮 HTTP 升到 92584/100000、projection pending 峰值降到 18579，但触发 8 次 PostgreSQL deadlock，不能采用该版本。
- [x] 将 Dispatcher 改为同一短事务内“锁候选→新语句快照只插缺失任务”，消除 Dispatcher/Worker 反向等锁；回归测试、全量 PostgreSQL、race、vet 通过。
- [x] 锁安全版本的新鲜 5000 req/s 轮 deadlock=0，已接受消息的 Realtime/Sync、cursor 和最终数据均完整；但 HTTP 仍只有 86183/100000，因此候选仍失败。
- [x] 新瓶颈定位为投影 Worker 的 `projection_store`：本轮平均 16.32ms，整批 27.30ms，4 Worker 的乐观满批上限约 9377 jobs/s，仍小于所需 10000 jobs/s。API acquisition 平均 107.94ms 是投影链路持续占用 PostgreSQL 的伴随现象，不单独当作根因。
- [ ] 先把 `projection_store` 继续拆成“标记 job 完成 / 锁定并标记 Outbox ready / 删除已完成 job”三类耗时，在不改变事务语义的前提下找出最慢 SQL，再只对它做 A/B。
- [ ] 分池降为后续诊断：只有投影吞吐已达到至少 10000 jobs/s，但 API 仍明显饥饿时，才固定总连接 24 比较 shared 24 与 API 18 + Worker 6。
- [ ] 候选当前保持 `inline/1` 默认；若压测方向成立，采用前补齐投影任务的独立 retry/dead 策略，并验证混合版本部署或明确要求先排空再切换。

## Sync/Outbox recipient 统一 A/B

- [x] 增加 `OUTBOX_PROJECTION_STORAGE=sync_events` 候选：正常准备事务只写 `user_message_events` 并标记 Outbox ready，已 ready 事件在重试时按 `message_id` 从 Sync 行重建 user/cursor。
- [x] 用真实 PostgreSQL 验证提交后崩溃恢复、Lease 丢失回滚、自发自收，以及 `recipients ↔ sync_events` 双向切换恢复。
- [x] 在 3500 req/s 稳定负载上按 `recipients A1 → sync_events B1 → sync_events B2 → recipients A2` 使用四个新鲜隔离库运行；每轮重启 PostgreSQL/Redis，四轮 HTTP、Realtime、Sync 都完整。
- [x] 直接目标指标方向一致：`prepare_store` 两轮中点从 `4.83ms` 降到 `1.81ms`（-62.4%），准备 Lane 从 `13.71ms` 降到 `11.14ms`（-18.7%）；每轮 140000 条重复 recipient 写入降为 0。Realtime/pending 峰值未同步改善，因此不宣称容量提升。
- [x] 将 `sync_events` 切为默认，保留 `OUTBOX_PROJECTION_STORAGE=recipients` 回退开关。全量测试、race、vet 通过；不显式设置存储开关的 1000 条真实链路 smoke 完成 HTTP 1000/1000、Realtime 4000/4000、Sync 2000/2000，且 `outbox_recipients=0`、pending/dead 为 0。
- [x] 用当前默认 `sync_events` 在新鲜隔离状态复核 5000 req/s：HTTP 99190/100000，Realtime/Sync 未在 30 秒窗口内完整，pending/oldest 峰值 `79891/45.861s`；API/Outbox acquisition P95 为 `250/100ms`，`prepare_store` 平均 95.23ms，`publish` 平均 1.78ms。Worker 在流量停止后 10 秒排空，最终数据完整。
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
  - [x] 切换默认 `sync_events` 后重复 5000 req/s 仍明确失败；去掉重复 recipient 写入未使该档达标，也不足以从非受控轮次声称容量提升。
  - [ ] 在 4000～5000 区间内使用新鲜状态复测，找到第一个可重复失败档；单轮 4000 通过不能当作稳定容量上限。
  - [ ] 同时比较 API/Outbox acquisition、`prepare_store`、`prepare_project_users`、HTTP、Realtime 和 pending/oldest，不能从一个平均阶段直接猜原因。

  inline 5000 已出现 API/Outbox acquisition P95 `250/100ms`；用户分片锁安全轮两侧 P95 均为 `250ms`，且 HTTP 只完成 86183/100000。当前先优化实测不足 10000 jobs/s 的投影链路；达到该门槛后若 API 仍饥饿，再执行固定总连接数的分池 A/B：

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
