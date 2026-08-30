# IM 真实链路基线测试

这个工具由你手动运行和观察，不是 CI 单元测试。它验证的链路是：

```text
管理员预置专用测试用户（不计时）
→ 正常 Login 创建 Session
→ 建立 WebSocket
→ POST /messages
→ PostgreSQL message + pending Outbox（HTTP 在这里即可返回）
→ Outbox Worker 按用户批量分配连续 seq
→ user_message_events + 可推送 Outbox payload 原子提交
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

报告同时保存用户数、设备数、并发数、请求超时、实时等待上限和实际负载时长，并记录测试前后的关键 Prometheus 计数器差值、四个 Outbox 批次阶段的耗时分布，以及压测期间 Outbox pending 和最老事件年龄的采样峰值。结束时的 pending、dead、goroutine 和常驻内存仍单独保留，不能用“最后 pending 为 0”替代过程峰值。

峰值默认每 `250ms` 抓取一次，可用 `-metrics-sample-interval` 调整。报告同时记录采样次数和错误数；阶段 P50/P95/P99 是 Prometheus Histogram 桶的上界近似值，不是原始逐批精确分位数。

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

服务端支持通过 `DATABASE_MAX_CONNECTIONS`、`OUTBOX_CONCURRENCY`、`OUTBOX_BATCH_SIZE` 和 `OUTBOX_PROJECTION_MODE` 设置连接池、推送并发、单次投影批量与投影实现。A/B 时一次只改一个参数：

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
