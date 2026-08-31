# Load-test evidence

[`reports/`](./reports/) 保存文档所引用的脱敏原始压测报告。压测方法、完整配置、指标含义和结论边界以 [`LOAD_TEST.md`](../LOAD_TEST.md) 为准；文件名本身不代表试验通过。

新运行先输出到项目根目录的 `loadtest-report*.json`（Git 忽略）。只有在配置、结果和脱敏状态都核对后，才命名并收录为可追溯证据；已收录报告不覆盖。

## 最新容量阶梯

| 目标速率 | 单轮结果 | 原始报告 |
| --- | --- | --- |
| 3000 req/s | HTTP、Realtime、Sync 完整，missing/duplicate/dead 为 0 | [`loadtest-rate-3000-db-acquire-r1.json`](./reports/loadtest-rate-3000-db-acquire-r1.json) |
| 3500 req/s | HTTP、Realtime、Sync 完整，missing/duplicate/dead 为 0 | [`loadtest-rate-3500-db-acquire-r1.json`](./reports/loadtest-rate-3500-db-acquire-r1.json) |
| 4000 req/s | HTTP、Realtime、Sync 完整，但准备 Lane 已接近预算 | [`loadtest-rate-4000-db-acquire-r1.json`](./reports/loadtest-rate-4000-db-acquire-r1.json) |
| 5000 req/s | 当前默认 `sync_events` 仍明确过载：HTTP 99190/100000，Realtime/Sync 未在核验窗口内完整；成功写入数据最终完整 | [`loadtest-rate-5000-sync-events-default-r1.json`](./reports/loadtest-rate-5000-sync-events-default-r1.json) |

3500 和 4000 均只是新鲜隔离库上的单轮证据，不等于稳定生产容量。当前可证实的区间是“4000 单轮完成，5000 明确过载”，精确且可重复的边界仍需在两者之间复测。

## 用户分片 Prepare 候选

[`loadtest-rate-5000-user-sharded-w4-r1.json`](./reports/loadtest-rate-5000-user-sharded-w4-r1.json) 是 4 个按用户分片 Prepare Worker 的首轮诊断。它只完成 82173/100000 HTTP，projection pending 峰值 115450，明确失败且比当前 inline 对照更差；报告用于定位候选实现中的 UUID 匹配全扫描，不能作为容量改善证据。停流追赶后，成功写入的 82173 条消息最终对应 164346 条连续 Sync 事件，projection jobs、Outbox pending/dead 均为 0。

[`loadtest-rate-5000-user-sharded-w4-r2.json`](./reports/loadtest-rate-5000-user-sharded-w4-r2.json) 只修复 UUID 点查。`projection_store` 从 193.56ms 降到 15.59ms，成功消息的 Realtime/Sync 与最终数据库计数完整；但 HTTP 仍仅 85032/100000，API acquisition 平均 105.85ms，共享 Pool 空闲等待累计 18370 秒。因此候选仍不采用，下一步只验证固定总连接数的 API/Worker 分池是否能避免互相饥饿。

## 默认存储链路复核

[`loadtest-default-sync-events-smoke.json`](./reports/loadtest-default-sync-events-smoke.json) 是不显式设置 `OUTBOX_PROJECTION_STORAGE` 的默认链路 smoke，用于证明运行时实际选中 `sync_events`、不再写 `outbox_recipients`，且 HTTP、Realtime 和 Sync 核验完整。它不是容量报告。

## 相关文档

- [`LOAD_TEST.md`](../LOAD_TEST.md)：方法、配置、A/B 结果与证据边界。
- [`OBSERVABILITY.md`](../OBSERVABILITY.md)：监控指标和排障顺序。
- [`TODO.md`](../TODO.md)：下一轮容量实验。
