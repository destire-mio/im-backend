# IM 真实链路基线测试

这个工具由你手动运行和观察，不是 CI 单元测试。它验证的链路是：

```text
管理员预置专用测试用户（不计时）
→ 正常 Login 创建 Session
→ 建立 WebSocket
→ POST /messages
→ PostgreSQL message + pending Outbox（HTTP 在这里即可返回）
→ Outbox Worker 按用户批量分配连续 seq
→ user_message_events + outbox_recipients + Outbox ready 原子提交
→ Hub / Channel / WebSocket
→ Sync API 持久性核验
```

实时链路诊断同时记录每条连接的 `send Channel` 深度、历史最高水位和 WebSocket 写耗时。压测客户端的收件跟踪表按消息 ID 分成 64 个锁分片，避免多个 WebSocket reader 因一把全局锁被误判为服务端慢连接。

## 它会写入什么

默认预置 10 个唯一测试用户，每个用户通过正常登录创建 2 个设备 Session，然后建立 20 条 WebSocket 连接并发送 500 条消息。测试用户的密码以与服务端相同的 Argon2id 格式写入，不保存明文报告。

工具只追加数据，不自动删除用户或消息。每次运行使用唯一 `runId`，可以用它定位该次数据。

## 运行前

1. 启动 PostgreSQL 和 Redis，并确认 schema 已初始化。
2. 启动 IM 服务端和 Outbox Worker。
3. 确认 API 默认在 `127.0.0.1:8080`，Prometheus 指标默认在 `127.0.0.1:9090/metrics`。
4. 尽量使用独立的本地测试数据库，不要指向生产库。

## 默认基线

在项目目录中运行：

```bash
go run ./cmd/im-loadtest \
  -allow-write \
  -allow-database im \
  -report ./loadtest-report.json
```

`-allow-database im` 必须和 `DATABASE_URL` 实际连接的数据库名完全一致，否则工具会在写入前拒绝运行。

如果端口不同：

```bash
go run ./cmd/im-loadtest \
  -allow-write \
  -allow-database im \
  -base-url http://127.0.0.1:8080 \
  -metrics-url http://127.0.0.1:9090/metrics \
  -users 10 \
  -devices 2 \
  -messages 500 \
  -concurrency 20 \
  -report ./loadtest-report.json
```

## 怎样看结果

终端会输出三个独立结论：

- `HTTP`：数据库写入请求的成功数、吞吐量和 P50/P95/P99 耗时。
- `Realtime`：每条成功消息是否到达发送方和接收方的每个 WebSocket，以及实时到达延迟。
- `Sync durability`：即使实时通知缺失，成功写入的消息是否仍能被发送方和接收方从 Sync API 找回。

结果的含义不要混在一起：

```text
HTTP PASS + Realtime FAIL + Sync PASS
= 数据没丢，但实时推送链路有问题

HTTP PASS + Sync FAIL
= 持久化或补拉正确性有问题，不能只当成 WebSocket 故障
```

如果首次请求遇到网络错误或 5xx，工具会使用原来的 `clientMessageId` 重试一次。第二次返回 200 表示首次已经写入，并由幂等机制找回原消息；这会计入 `idempotent_recovered`，不会被误判为第二条消息。

报告同时保存用户数、设备数、并发数、请求超时、实时等待上限和实际负载时长，并记录测试前后的关键 Prometheus 计数器差值、四个 Outbox 批次阶段的耗时分布、`databaseAcquireDurations` 中 API/Outbox 各自获取共享 Pool 连接的 success/error 耗时分布，以及压测期间 Outbox pending 和最老事件年龄的采样峰值。结束时的 pending、dead、goroutine 和常驻内存仍单独保留，不能用“最后 pending 为 0”替代过程峰值。

峰值默认每 `250ms` 抓取一次，可用 `-metrics-sample-interval` 调整。报告同时记录采样次数和错误数；Outbox 阶段与数据库连接获取的 P50/P95/P99 都是 Prometheus Histogram 桶的上界近似值，不是原始逐次精确分位数。数据库 acquisition Histogram 从调用 Pool 到成功或失败为止，覆盖等待空闲连接、创建/检查连接等耗时，但不包含拿到连接之后的 SQL 执行。

## 固定速率压测

默认模式是闭环：最多由 `-concurrency` 个发送者持续发送，前一个请求越慢，后续请求开始得也越慢。它适合验证完整链路，但会在服务变慢时自动降低施压速度，不能单独用来判断容量上限。

设置 `-rate` 后切换为固定速率模式：工具按指定速率计划请求开始时间，`-concurrency` 变成最大在途请求数。例如先运行 500 req/s：

```bash
go run ./cmd/im-loadtest \
  -allow-write \
  -allow-database im \
  -users 10 \
  -devices 2 \
  -messages 10000 \
  -rate 500 \
  -concurrency 100 \
  -delivery-wait 30s \
  -report ./loadtest-rate-500.json
```

压测器会把 HTTP 连接池的 `MaxIdleConns`、`MaxIdleConnsPerHost` 和 `MaxConnsPerHost` 设为 `-concurrency`，使连接在固定速率测试中复用。否则 Go 默认每个目标只保留 2 个空闲连接，高并发下会制造大量短连接，并可能以 `can't assign requested address` 先耗尽压测机临时端口；这种失败属于压测器瓶颈，不能算作服务端容量。

报告中的字段需要分开理解：

- `targetRateRps`：压测器计划每秒开始多少个请求。
- `httpThroughputRps`：最终成功请求数除以整段负载耗时，不等同于目标速率。
- `droppedStarts`：到计划时间时 `-concurrency` 个在途槽已经全部占满，因此压测器没有发出该请求；它不是服务端返回的 HTTP 错误。
- `messagesFailed`：包括真正发出后失败的请求和未能开始的计划请求；`requestErrors` 会标明具体原因。

测容量时必须同时观察成功率、P95/P99、`droppedStarts`、Realtime 延迟、Outbox 堆积与数据库连接池等待。只看吞吐量会上报一个看似更高、但已经积压或丢弃计划请求的数字。

## 当前测试边界

- 这是第一个单实例基线，不是容量上限。
- 它检查结果是否到达，但还没有注入 Redis 断联、Worker 崩溃、慢 WebSocket 或多实例故障。
- Login 仍经过生产限流链路。默认 20 次 Login 低于当前每 IP 30 次/分钟的上限；如果同一 Redis 近期已有 Login 请求，工具会显示 `Retry-After` 并停止，不会绕过限流。
- 脚本不发送 ACK：它只能证明 Sync API 返回数据，不能伪装真实手机已将数据落盘。ACK 需要在真实客户端或单独的客户端持久化故障实验中验证。

## Outbox 并发 A/B

服务端支持通过 `DATABASE_MAX_CONNECTIONS`、`OUTBOX_CONCURRENCY`、`OUTBOX_BATCH_SIZE`、`OUTBOX_PROJECTION_MODE` 和 `OUTBOX_PROJECTION_STORAGE` 设置连接池、推送并发、单次投影批量、投影 SQL 与投影结果存储。A/B 时一次只改一个参数：

```text
对照组：DATABASE_MAX_CONNECTIONS=24 OUTBOX_CONCURRENCY=8
实验组：DATABASE_MAX_CONNECTIONS=24 OUTBOX_CONCURRENCY=16
```

两组必须使用相同的 `users`、`devices`、`messages`、`concurrency` 和 `delivery-wait`。报告会保存 Worker 并发、批量、连接池上限，以及空池等待次数和累计等待时间。

`OUTBOX_BATCH_SIZE` 不是越大越好。批次越大，单个用户计数器的更新次数越少，但一次事务写入的同步记录和 Outbox payload 越多，会形成更明显的 WAL/索引写入突刺，并扩大 Lease 内需要完成的工作。

当前默认 Batch 为 `64`。在两个同 schema、初始为空的隔离库上，保持 Pool 24、Worker 8、10 个热点用户、20 条 WebSocket 和 3000 req/s 不变时，Batch 64 相比 32 将 Realtime P95 从约 19.84 秒降到 5.29 秒，过程 pending 峰值从 37467 降到 16028，数据库空池累计等待从约 132.30 秒降到 8.75 秒；HTTP、Realtime、Sync 均完整，missing、duplicate、dead 为 0。报告为 `loadtest-rate-3000-outbox-b32-fresh.json` 与 `loadtest-rate-3000-outbox-b64-fresh.json`。

当前默认 publish 并发为 `16`。主机 CPU 恢复空闲后，在两个由同一空库克隆的隔离库上保持 Pool 24、Batch 64 和上述负载不变，并发 16 相比 8 将 Realtime P95 从约 2.71 秒降到 0.63 秒，pending 峰值从 8600 降到 2256，oldest-age 峰值从 2.87 秒降到 0.76 秒，数据库空池累计等待从 25.03 秒降到 19.21 秒。两组 60000 个 HTTP 请求、240000 次 Realtime 投递和 120000 条 Sync 事件均完整，missing、duplicate、dead 为 0。代价是结束采样 RSS 约增加 25.5 MB。报告为 `loadtest-rate-3000-outbox-b64-w8-clean-rerun.json` 与 `loadtest-rate-3000-outbox-b64-w16-clean-rerun.json`。

这个结果只证明 `64` 优于本次热点模型中的 `32`，不证明继续放大仍会改善；历史上的 `256` 曾显著恶化。3000 req/s 下 Batch 64 仍有约 5.34 秒的 oldest-age 峰值，因此容量边界尚未消失。

## 当前下一容量边界

采用 Batch 64、publish 并发 16 后，Pool 24 在 3000 req/s 下可以完整处理 60000 条消息；细分后的平均 prepare 阶段为：

```text
decode 0.12ms + begin 0.31ms + project_users 5.44ms
+ encode 0.06ms + store 2.69ms + commit 0.71ms
= prepare 约 9.35ms / 64 条
```

提高到 3500 req/s 时，Pool 24 出现 222 个 dropped starts，空池累计等待约 1232 秒，HTTP P95 约 140ms，Realtime P95 约 13.13 秒，pending 峰值约 40200。把 Pool 单独提高到 32 后，70000 个 HTTP 请求全部成功，HTTP P95 降到约 26.70ms，空池等待降到约 177.59 秒；但 Realtime P95 仍约 12.59 秒，pending 峰值仍约 41512。

因此 Pool 24 是 3500 req/s 的入口限制之一，但扩大 Pool 只解除 HTTP 争用，没有提高 Outbox 主循环吞吐。下一层瓶颈是串行 `PrepareBatch`：稳态下 `project_users` 的逐用户 SQL 占 prepare 最大部分；接近边界时，批量回写 Outbox JSONB payload 的 `prepare_store` 还出现过接近 1 秒的长尾。继续优化前应先减少 prepare 的逐用户数据库往返和 Outbox 行更新成本，而不是继续增加 publish 并发或 Channel 容量。

## Projection SQL 四对顺序平衡 A/B

为验证 `project_users` 的成本是否真的来自“每用户一次 SQL”，保留两个可切换实现：

```text
A：OUTBOX_PROJECTION_MODE=per_user（原实现，回退开关）
B：OUTBOX_PROJECTION_MODE=bulk（当前默认）
```

B 在同一事务内先批量确保 counter 行存在，再按 `user_id` 一次性 `FOR UPDATE`，最后一次批量更新 counter 并插入 `user_message_events`。它没有放松连续序号、固定锁顺序或 Outbox payload 原子提交边界。

四对均从同一个空库模板克隆独立数据库，保持 Pool 24、Batch 64、publish 并发 16、10 用户、20 WebSocket、60000 消息、3000 req/s、并发 1000 和 30 秒实时等待不变。运行顺序为 `A→B、B→A、A→B、B→A`，使两种模式各有两次先跑和两次后跑。四轮 A、四轮 B 的 HTTP、Realtime、Sync 均完整，missing、duplicate、unexpected、dead 均为 0。

| 四轮中位数 | A：per_user | B：bulk | 变化 |
| --- | ---: | ---: | ---: |
| projection SQL / 批 | 10 | 3 | -70.0% |
| projection SQL 客户端总耗时 / 批 | 5.51 ms | 3.64 ms | -34.0% |
| `prepare_project_users` | 5.52 ms | 3.65 ms | -33.8% |
| `prepare` 总计 | 9.61 ms | 8.00 ms | -16.8% |
| Realtime P95 | 2.862 s | 2.583 s | -9.7% |
| pending 峰值 | 9120 | 7517 | -17.6% |
| oldest age 峰值 | 3.043 s | 2.666 s | -12.4% |
| HTTP P95 | 10.24 ms | 26.77 ms | B 较差 |
| Pool 空池累计等待 | 55.66 s | 212.45 s | B 较差 |

`prepare_project_users` 在 4/4 轮中都下降，且 A 的四个值 `5.20/5.66/5.37/5.90ms` 与 B 的 `3.94/3.36/3.85/3.45ms` 没有重叠。因此可以确认：逐用户 SQL 的固定语句/往返成本是真实的，批量实现每批稳定节省约 1.87ms 客户端观测查询时间；但该指标仍包含 PostgreSQL 执行、锁等待、驱动和结果读取，不能解释为纯网络 RTT。

整条链路仍受同一 PostgreSQL 连续写入状态影响：四对中的第二轮无论是 A 还是 B 都明显更慢，并伴随 `prepare_store`、mark 和连接池等待上升。四对 A/B 结束时没有立即切换默认值；随后在保持 `per_user` 回退能力的前提下采用 B，并用新的空库继续复核，结果见下一节。

报告：`loadtest-rate-3000-projection-ab-a1-per-user.json` 至 `a4-per-user.json`，以及 `loadtest-rate-3000-projection-ab-b1-bulk.json` 至 `b4-bulk.json`。

## 批量投影采用后的容量复核

代码默认值、`.env.example`、指标测试和本地 8080 服务均已切换到 `bulk`；Pool 24、Batch 64、publish 并发 16 保持不变，`per_user` 继续作为显式回退模式。

3000 req/s、60000 条、Pool 24 的新空库主链完整通过：

```text
HTTP                         60000 / 60000，P95 8.15ms
Realtime                     240000 / 240000，P95 0.762s
Sync                         120000 / 120000
pending / oldest 峰值        2520 / 0.837s
Pool 空池累计等待            99.63s
claim / prepare / publish / mark
                             2.14 / 6.51 / 7.98 / 2.72ms
project_users / prepare_store
                             3.31 / 1.85ms
```

3500 req/s 时，Pool 24 仍先限制 HTTP：65708/70000 成功、4292 个 dropped starts、HTTP P95 369.34ms、空池等待 3414.76s。这一轮不能用来单独判断 Worker 上限。

将 Pool 单独提高到 32 后，70000/70000 HTTP 全部成功，HTTP P95 5.83ms，空池等待仅 20.72s；但 Realtime P95 仍为 19.10s，pending / oldest 峰值为 18288 / 20.11s。阶段分解为：

```text
prepare_project_users        3.30ms，P95 <= 10ms
prepare_store               18.22ms，P95 <= 250ms，P99 <= 500ms
prepare 总计                22.76ms
publish                      8.05ms
mark_published               3.62ms
Channel 最高水位             5 / 256
```

因此采用 bulk 后，连续序号分配已不再是 3500 req/s 下的最大阶段；新的明确瓶颈是同一 Sync/Outbox 准备事务里的 `prepare_store`，即把带 recipients/cursor 的完整 JSONB 回写 `outbox_events`。当轮 70000 个 Outbox 事件发生 210000 次更新（claim、payload 回写、published 各一次），仅 80867 次为 HOT update；结束时 Outbox heap 约 65MB、含索引约 79MB，并触发过一次 autovacuum。现有证据确认“Outbox 行更新/JSONB 回写阶段”发生长尾，WAL、tuple 扩张、页写入和 vacuum 的具体占比仍需在下一方案 A/B 中分离。

报告：

- `loadtest-rate-3000-bulk-default.json`
- `loadtest-rate-3500-bulk-default-p24.json`
- `loadtest-rate-3500-bulk-default-p32.json`

## 结构化 recipients 拆分 A/B

为去掉带 recipients/cursor 的完整 JSONB 二次回写，新增两种可切换存储：

```text
A：OUTBOX_PROJECTION_STORAGE=jsonb（旧实现，回退开关）
B：OUTBOX_PROJECTION_STORAGE=recipients（当前默认）
```

B 将 `user_message_events`、`outbox_recipients` 和 `outbox_events.ready_at` 放在同一事务提交；Publisher 只在事务提交后运行。若事务提交后、publish 前崩溃，重启 Worker 从结构化 recipients 重建内存 payload；若 Lease 已丢失，带 `lock_token` 的 ready 更新影响 0 行，整个事务回滚。网络发送仍在事务外，发送后、标记前崩溃仍可能产生重复通知，客户端继续按 event/message/cursor 去重。

3500 req/s 的 A/B 使用同一空库模板克隆四个隔离库，固定 Pool 32、Batch 64、publish 并发 16、10 个热点用户、20 条 WebSocket、70000 条消息、并发 1000 和 30 秒实时等待，顺序为 `A1 → B1 → B2 → A2`。每轮使用独立 Redis DB，避免 Login 限流互相污染。四轮 HTTP、Realtime、Sync 全部完整，dropped、missing、duplicate、unexpected、dead 均为 0。

| 两轮中位数 | A：JSONB | B：recipients | 变化 |
| --- | ---: | ---: | ---: |
| `prepare_store` | 11.31 ms | 7.88 ms | -30.3% |
| `prepare` 总计 | 16.34 ms | 12.71 ms | -22.2% |
| Realtime P95 | 16.427 s | 11.159 s | -32.1% |
| oldest age 峰值 | 16.641 s | 11.177 s | -32.8% |
| pending 峰值 | 36071 | 34790 | -3.6% |
| Pool 空池累计等待 | 328.49 s | 53.36 s | -83.8% |
| `mark_published` | 5.46 ms | 6.39 ms | +16.9% |

A 两轮本身波动明显：`prepare_store` 分别为 `17.03ms` 和 `5.59ms`，因此不能把两轮中位数当成稳定容量数字；但 B 两轮为 `8.01ms` 和 `7.76ms`，且端到端 Realtime P95 两轮都低于 A。准确结论是：移除 JSONB payload 回写降低了平均 prepare 与积压年龄，并改善了本次边界负载的稳定性，但 3500 req/s 仍超过实时稳态吞吐。

采用 B 后，Pool 24 的新空库 3000 req/s 默认复核完整通过：HTTP 60000/60000、Realtime 240000/240000、Sync 120000/120000；HTTP P95 `3.89ms`、Realtime P95 `0.584s`、pending / oldest 峰值 `1818 / 0.607s`，结束时 pending/dead 为 `0/0`。因此新默认没有让原有稳定档退化。

### 拆分后的下一瓶颈

3500 req/s 下 B 两轮平均阶段的中位数约为：

```text
claim 2.18ms + prepare 12.71ms + publish 6.27ms + mark 6.39ms
= 27.55ms / 64 条
```

3500 req/s 要求每 64 条的串行批次关键路径不高于约 `18.29ms`，当前 Worker 主循环仍跟不上。prepare 内最大子阶段仍是 `prepare_store`（约 `7.88ms`），只是成本已从 JSONB 回写转成“插入两份结构化 recipient + 更新 ready”。每轮 70000 条消息会写入 140000 条 `user_message_events` 和 140000 条 `outbox_recipients`，同时 `outbox_events` 仍有 claim、ready、published 共 210000 次更新；`outbox_recipients` 约占 20MB，而 user/cursor 映射已存在于约 25MB 的 `user_message_events`。

这说明去掉重复的 `outbox_recipients` 写入仍是减少 prepare 写放大的候选方向，但在继续改变数据模型前，先用执行模式 A/B 验证四个阶段的串行调度本身是否限制批次吞吐。

报告：

- `loadtest-rate-3500-storage-ab-a1-jsonb.json`
- `loadtest-rate-3500-storage-ab-b1-recipients.json`
- `loadtest-rate-3500-storage-ab-b2-recipients.json`
- `loadtest-rate-3500-storage-ab-a2-jsonb.json`
- `loadtest-rate-3000-recipients-default.json`

## Outbox 两段流水线 A/B

连续运行的 Worker 保留两种可切换执行模式：

```text
A：OUTBOX_EXECUTION_MODE=serial
   claim → prepare → publish → mark，完成整批后才领取下一批

B：OUTBOX_EXECUTION_MODE=pipeline（当前默认）
   准备阶段：claim → prepare
   投递阶段：publish → mark
```

B 使用无缓冲 Channel 交接已准备批次。投递第 N 批时，准备协程可以处理第 N+1 批；Channel 不承担持久化，SQL 中的 pending/ready/published 状态仍是恢复依据。无缓冲交接使系统最多同时持有一批正在准备和一批正在投递的 Lease，Publisher 变慢时不会继续预领任意多批。`RunOnce` 仍执行一个完整串行批次，供测试和维护调用。

故障边界没有放松：prepare 事务提交前失败会回滚；提交后、publish 前退出时，事件仍处于可恢复的 ready 状态；publish 之后、mark 之前失败仍可能重复通知，客户端继续按 message/event/cursor 去重。网络 publish 仍在数据库事务外。

3500 req/s 使用同一空库模板克隆四个隔离数据库，固定 Pool 32、Batch 64、publish 并发 16、10 个热点用户、20 条 WebSocket、70000 条消息、并发 1000 和 30 秒等待，顺序为 `A1 → B1 → B2 → A2`。四轮 HTTP 都是 `70000/70000`，dropped、duplicate、unexpected、dead 都为 0；但 A1 和 B1 都没有在 30 秒窗口内清空 pending，因此这两轮的 Realtime/Sync 必须记为失败，不能把已观察消息的 P95 当成完整交付 SLO。

| 两轮中位数 | A：serial | B：pipeline | 变化 |
| --- | ---: | ---: | ---: |
| 已观察 Realtime P95 | 23.480 s | 16.226 s | -30.9% |
| pending 峰值 | 39315 | 27334 | -30.5% |
| oldest age 峰值 | 23.500 s | 22.290 s | -5.1% |
| HTTP P95 | 31.71 ms | 21.77 ms | -31.3% |
| `prepare` | 24.03 ms | 35.12 ms | +46.1% |
| `publish` | 6.95 ms | 14.93 ms | +114.6% |
| `mark_published` | 6.33 ms | 5.61 ms | -11.3% |

流水线在两组配对中都降低了 Realtime P95 和 pending 峰值，但单阶段耗时反而上升，说明重叠执行增加了 PostgreSQL、Redis、CPU 或调度等共享资源竞争；它提高的是批次重叠吞吐，不是让单次 SQL 或 publish 变快。A1 结束时 serial 尚有 3696 个 pending，B1 结束时 pipeline 尚有 18864 个；这种高波动也说明 3500 req/s 仍超过当前稳定边界，不能据两轮中位数宣称容量已经达到 3500。

为确认稳定档没有退化，又在 Pool 24、60000 条、3000 req/s 下各跑一轮同条件对照。两轮 HTTP、Realtime、Sync 全部完整，missing、duplicate、unexpected、dead 均为 0：

| 3000 req/s | serial | pipeline | 变化 |
| --- | ---: | ---: | ---: |
| Realtime P95 | 3.312 s | 2.234 s | -32.5% |
| pending 峰值 | 10332 | 7004 | -32.2% |
| oldest age 峰值 | 3.444 s | 2.337 s | -32.1% |
| HTTP P95 | 5.51 ms | 9.43 ms | +71.2% |
| `claim + prepare` | 11.85 ms | 16.72 ms | +41.1% |
| `publish + mark` | 11.45 ms | 20.49 ms | +79.0% |

因此采用 pipeline 作为默认值，同时保留 serial 回退。代价是本轮 HTTP P95 和各阶段墙钟时间上升；收益是3000稳定档的实时延迟与积压峰值下降。流水线模式下不再把四阶段耗时简单相加判断吞吐，而应分别比较两条 Lane：3000负载下准备 Lane 约 `16.72ms`，投递 Lane 约 `20.49ms`。3500 req/s 对 Batch 64 的预算约为 `18.29ms`，所以当前下一瓶颈首先是投递 Lane，其中 `publish` 约 `17.01ms`，再加 `mark` 后超过预算；prepare 中的结构化重复写入仍是第二个接近边界的候选。

报告：

- `loadtest-rate-3500-pipeline-ab-a1-serial.json`
- `loadtest-rate-3500-pipeline-ab-b1-pipeline.json`
- `loadtest-rate-3500-pipeline-ab-b2-pipeline.json`
- `loadtest-rate-3500-pipeline-ab-a2-serial.json`
- `loadtest-rate-3000-pipeline-default.json`
- `loadtest-rate-3000-pipeline-ab-serial.json`

## Presence 批内快照

投递阶段保留一个可回退开关：

```text
A：OUTBOX_BATCH_PRESENCE_LOOKUP=false
   每个 event 逐 recipient 执行 SMEMBERS + MGET

B：OUTBOX_BATCH_PRESENCE_LOOKUP=true（当前默认）
   取出一批 event 的 recipient
   → 按 user_id 去重
   → 第一段 Redis pipeline 批量 SMEMBERS
   → 第二段 Redis pipeline 批量 MGET
   → 本批并发 publish 共享这份不可变快照
   → 批次结束即丢弃
```

这不是跨批长期缓存，因此不需要额外失效协议。代价是同一批处理期间的 Presence 变化可能不会进入当前快照；WebSocket 仍只是实时通知，客户端必须用 Sync 恢复。Redis 预取失败时，批内 Router 仍会先尝试本地 Hub 投递，然后返回可重试错误，不会把 Redis 故障变成本地通知的前置条件。

`outbox_stage_duration_seconds{stage="publish"}` 包含批内预取，`stage="publish_prepare"` 单独显示快照准备耗时；单事件 `outbox_publish_duration_seconds` 不包含这段共享成本。`outbox_batch_presence_users_total / outbox_batch_presence_batches_total` 用来观察实际唯一用户数/批。

集成测试已确认：重复 user ID 只生成两段 Redis pipeline；Redis 不可用时本地 Hub 仍先收到通知；跨实例 Outbox 投递可以通过批内快照到达远端 WebSocket。

### 10 个热点用户 A/B

四轮使用独立空库和 Redis DB，固定 Pool 24、Batch 64、publish 并发 16、pipeline + bulk + recipients、10 个用户、20 条 WebSocket、60000 条消息、3000 req/s 和并发 1000，顺序为 `A1 → B1 → B2 → A2`。四轮 HTTP 都是 `60000/60000`，Realtime 都是 `240000/240000`，Sync 都是 `120000/120000`；dropped、missing、duplicate、unexpected、dead 均为 0。

| 两轮中点 | A：逐 recipient 查询 | B：批内快照 | 变化 |
| --- | ---: | ---: | ---: |
| Presence 解析次数/轮 | 120000 | 9388.5 | -92.2% |
| 唯一用户/批 | 不适用 | 10.0 | — |
| `publish_prepare` | 0.003 ms | 0.628 ms | 新增共享成本 |
| 整批 `publish` | 16.54 ms | 1.68 ms | -89.8% |
| Realtime P95 | 7.528 s | 1.195 s | -84.1% |
| pending 峰值 | 22898 | 3898 | -83.0% |
| oldest age 峰值 | 7.652 s | 1.301 s | -83.0% |

A 的两轮 Realtime P95 为 `4.916s / 10.139s`，B 为 `0.539s / 1.851s`，本机端到端波动仍然明显；但直接的 `publish` 阶段在 A 两轮为 `17.75ms / 15.32ms`，B 两轮为 `1.70ms / 1.67ms`，方向和幅度都一致。因此可以确认热点用户下批内快照消除了当前投递阶段的重复 Presence 往返，但不把四轮单机数字外推为生产容量。

### 优化后的下一瓶颈

B 两轮的平均阶段中点组成为：

```text
准备 Lane：claim 4.26ms + prepare 14.64ms = 18.90ms
投递 Lane：publish 1.68ms + mark 4.80ms = 6.48ms
```

所以原先约 `20.49ms` 的投递 Lane 已不是首要限制，下一瓶颈转到准备 Lane。其中最大单个子阶段是 `prepare_store` 约 `6.75ms`，其次是 `prepare_project_users` 约 `5.68ms`；3500 req/s 对 Batch 64 的预算约 `18.29ms`，而准备 Lane 已约 `18.90ms`。下一个数据模型候选仍是减少 `prepare_store` 中 `user_message_events` 与 `outbox_recipients` 的重复 user/cursor 写入，但需要单独设计、A/B 和 push，本步骤不继续改数据模型。

更多不同用户、缓存命中率下降的实验已列入 [TODO.md](./TODO.md)。

报告：

- `loadtest-rate-3000-presence-ab-a1-per-recipient.json`
- `loadtest-rate-3000-presence-ab-b1-batch.json`
- `loadtest-rate-3000-presence-ab-b2-batch.json`
- `loadtest-rate-3000-presence-ab-a2-per-recipient.json`

## Presence 优化后的升档诊断

为了继续寻找下一个瓶颈，先尝试 3200 req/s，再用 3000 req/s、Pool 32/24 和重启 PostgreSQL/Redis 后的 Pool 24 做三轮对照。四轮都使用当前默认的 pipeline + bulk + recipients + 批内 Presence 快照、Batch 64、10 个热点用户和 30 秒投递等待。

这四轮都不能作为新容量证据：每轮都出现大量 `not started: max in-flight requests reached`，即压测器因为 1000 个并发槽已占满而未发出部分目标请求。报告中的 Realtime/Sync `passed` 只证明“已经成功写入的消息”最终完整交付，不代表目标消息总数全部完成。

| 轮次 | Pool | HTTP 成功 / 目标 | dropped starts | 实际吞吐 | HTTP P95 | `publish` | `prepare_begin` | `claim` | `mark` | 空池获取次数 | 空池累计等待 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3200 | 32 | 47817 / 64000 | 16183 | 2346.8 req/s | 555.80 ms | 1.80 ms | 11.07 ms | 14.77 ms | 19.25 ms | 92528 | 17959.03 s |
| 3000 对照 | 32 | 51215 / 60000 | 8785 | 2504.2 req/s | 489.51 ms | 2.06 ms | 8.17 ms | 12.83 ms | 18.47 ms | 75136 | 13187.34 s |
| 3000 对照 | 24 | 43476 / 60000 | 16524 | 2113.8 req/s | 725.74 ms | 2.13 ms | 11.35 ms | 14.56 ms | 20.50 ms | 67943 | 14822.99 s |
| 3000，重启依赖后 | 24 | 32368 / 60000 | 27632 | 1563.5 req/s | 1230.69 ms | 2.73 ms | 15.43 ms | 21.79 ms | 25.26 ms | 49786 | 16450.36 s |

“空池累计等待”是所有并发 goroutine 等待数据库连接的时长之和，不是单轮墙钟时长，所以会远大于压测实际持续时间。重启依赖后没有恢复此前 3000 req/s 的稳定基线，因此本轮没有继续跑 3500 req/s；在更低档已经不满足请求完整性时，3500 的结果无法用于判定后端容量。

四轮仍提供了同方向的过载诊断：批内 Presence 优化后的 `publish` 一直只有 `1.80～2.73ms`，没有重新成为首要限制；与此同时 HTTP P95、`prepare_begin`、`claim`、`mark_published` 和空池等待一起放大。这说明当前环境进入过载时，先暴露的是 HTTP 与 Outbox Worker 共用 PostgreSQL Pool 后的连接获取等待和事务占用，而不是 Redis Presence 或 Channel。

`prepare_store` 四轮仍为 `10.77～12.38ms`，它依然是准备事务内部值得减少的写入成本，但仅凭这四轮不能说它是第一个把系统压垮的因素。下一步应先把“等待连接”和“拿到连接后的 SQL/事务执行”分开计时，再在数据库总连接数保持不变的前提下，对比共享 Pool 与 API/Worker 分池；只有重新获得可重复的 3000 req/s 基线后，才继续升档并判定新容量边界。

报告：

- `loadtest-rate-3200-presence-default-r1.json`
- `loadtest-rate-3000-presence-capacity-control-r1.json`
- `loadtest-rate-3000-presence-pool24-control-r2.json`
- `loadtest-rate-3000-presence-postrestart-control-r1.json`

## 按 workload 拆分连接获取后的 3000 复核

2026-08-31 使用新的隔离数据库和空 Redis DB，固定 Pool 24、Batch 64、publish 并发 16、pipeline + bulk + recipients + 批内 Presence 快照、10 个热点用户、20 条 WebSocket、60000 条消息、3000 req/s 和并发 1000。该轮 HTTP `60000/60000`、Realtime `240000/240000`、Sync `120000/120000` 全部完整，dropped、failed、missing、duplicate、unexpected、dead 均为 0，因此可作为有效基线。

| acquisition workload | 次数 | 平均耗时 | P50 桶上界 | P95 桶上界 | P99 桶上界 |
| --- | ---: | ---: | ---: | ---: | ---: |
| API | 121800 | 0.111 ms | 0.01 ms | 0.10 ms | 5.00 ms |
| Outbox | 2818 | 0.044 ms | 0.01 ms | 0.01 ms | 2.50 ms |

Pool 整体发生 6442 次空池获取，所有 goroutine 的空池累计等待为 `13.58s`；同期共有 124788 次连接获取，所以不能只凭“空池次数不为 0”判断连接池已饱和。分组 Histogram 显示绝大多数 API/Outbox acquisition 都在亚毫秒内完成，且没有 canceled acquire。因此在这轮有效的 3000 req/s 稳态中，共享 Pool 不是当前瓶颈，也没有证据支持立即做 API 18 + Worker 6 的分池改造。

端到端和 Worker 阶段为：HTTP P95 `8.01ms`、Realtime P95 `0.784s`、pending/oldest 峰值 `2441/0.816s`；`claim=4.06ms`、`prepare=14.41ms`、`publish=1.82ms`、`mark=4.47ms`。准备 Lane 约 `18.47ms`，低于 3000 req/s 下每 64 条约 `21.33ms` 的到达预算；准备内部仍以 `prepare_store=6.71ms` 和 `prepare_project_users=5.71ms` 最大，但当前结果只说明它们是下一次升档应重点观察的阶段，不说明已经形成稳态瓶颈。

此前四轮未复现基线的诊断仍保留为异常环境证据，不能据此证明共享 Pool 持续争用。下一轮应在新鲜隔离状态升到 3200 req/s，同时观察 acquisition 和 SQL 阶段；只有 acquisition 明显放大时才进入固定总连接数的分池 A/B。

报告：

- `loadtest-rate-3000-db-acquire-r1.json`

## 3500 req/s 单轮升档

2026-08-31 按用户要求跳过 3200，直接使用新的隔离数据库和空 Redis DB 跑 3500 req/s。除目标速率和消息数改为 3500 req/s、70000 条外，Pool 24、Batch 64、publish 并发 16、pipeline + bulk + recipients + 批内 Presence 快照、10 个热点用户、20 条 WebSocket、并发 1000 和 30 秒投递等待均与上一轮 3000 相同。

该轮 HTTP `70000/70000`、Realtime `280000/280000`、Sync `140000/140000` 全部完整，dropped、failed、missing、duplicate、unexpected、dead 均为 0；HTTP P95/P99 为 `10.20/41.46ms`，Realtime P95 为 `0.625s`，pending/oldest 峰值为 `2350/0.691s`，结束时 pending/dead 为 `0/0`。因此这是一次有效的 3500 完整通过结果，但只有一轮，不能单独宣称 3500 已是可重复容量。

| acquisition workload | 3000 P95 | 3500 P95 | 3000 平均 | 3500 平均 |
| --- | ---: | ---: | ---: | ---: |
| API | 0.10 ms | 2.50 ms | 0.111 ms | 0.520 ms |
| Outbox | 0.01 ms | 0.01 ms | 0.044 ms | 0.130 ms |

Pool 整体空池累计等待由 3000 单轮的 `13.58s` 增至 `74.29s`，API acquisition 的 P95/P99 上界也升至 `2.50/25ms`，说明 API 侧已经出现更明显的连接等待信号；但 Outbox P95 仍不高于 `0.01ms`，没有出现 Worker 被共享 Pool 饿死。该轮没有 canceled acquire，且端到端正确性和延迟仍完整，因此“连接等待开始放大”不能等同于“连接池已经成为容量瓶颈”。

Worker 平均阶段为 `claim=3.37ms`、`prepare=11.19ms`、`publish=1.60ms`、`mark=3.42ms`。准备 Lane 约 `14.56ms`，仍低于 3500 req/s 下每 64 条约 `18.29ms` 的预算；`prepare_store=5.05ms`、`prepare_project_users=4.46ms` 也没有在本轮形成积压。下一步若继续寻找边界，应使用新鲜状态继续升档或先重复 3500；只有出现可重复失败后，才能根据当轮 acquisition/SQL 指标选择分池或 SQL A/B。

报告：

- `loadtest-rate-3500-db-acquire-r1.json`

## 4000 req/s 单轮升档

继续使用新鲜隔离数据库和空 Redis DB，只将负载提高为 4000 req/s、80000 条消息，其余条件与 3500 单轮相同。该轮 HTTP `80000/80000`、Realtime `320000/320000`、Sync `160000/160000` 全部完整，dropped、failed、missing、duplicate、unexpected、dead 均为 0；结束时 pending/dead 为 `0/0`。因此 4000 在本轮完整通过，但仍不能据单轮结果宣称可重复容量。

| 指标 | 3500 单轮 | 4000 单轮 |
| --- | ---: | ---: |
| HTTP P95 / P99 | 10.20 / 41.46 ms | 37.38 / 85.51 ms |
| Realtime P95 | 0.625 s | 1.768 s |
| pending / oldest 峰值 | 2350 / 0.691 s | 7190 / 1.799 s |
| API acquisition 平均 / P95 | 0.520 / 2.50 ms | 1.944 / 25.00 ms |
| Outbox acquisition 平均 / P95 | 0.130 / 0.01 ms | 0.466 / 1.00 ms |
| Pool 空池累计等待 | 74.29 s | 317.76 s |

连接等待已从“预警”变为明显压力：API acquisition P95 增大十倍，Outbox P95 也从 `0.01ms` 升至 `1ms`，且 Pool 空池累计等待约为 3500 单轮的 4.3 倍。它已经实质影响 HTTP 长尾，但本轮没有 canceled acquire，Worker 也没有被饿死或留下最终积压，所以仍不能直接断言“分池就是容量修复”。

Worker 平均阶段为 `claim=3.63ms`、`prepare=11.67ms`、`publish=1.60ms`、`mark=4.21ms`。准备 Lane 约 `15.30ms`，而 4000 req/s 下每 64 条预算为 `16.00ms`，只剩约 `0.70ms` 余量；投递 Lane 约 `5.81ms`。`prepare_store=5.17ms` 和 `prepare_project_users=4.34ms` 没有像 acquisition 一样相对 3500 明显放大，但它们组成的准备 Lane 已接近吞吐预算。

因此 4000 当前同时出现两项边界信号：共享 Pool acquisition 推高 HTTP 长尾，准备 Lane 接近 Worker 吞吐预算。下一步应重复 4000 验证波动，或继续升档捕获第一个失败；若 acquisition 放大可重复，再用固定总连接数的共享/分池 A/B 判断争用因果，不能在单轮通过后直接改架构。

报告：

- `loadtest-rate-4000-db-acquire-r1.json`
