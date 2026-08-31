# Load-test evidence

[`reports/`](./reports/) 保存文档所引用的脱敏原始压测报告。压测方法、完整配置、指标含义和结论边界以 [`LOAD_TEST.md`](../LOAD_TEST.md) 为准；文件名本身不代表试验通过。

新运行先输出到项目根目录的 `loadtest-report*.json`（Git 忽略）。只有在配置、结果和脱敏状态都核对后，才命名并收录为可追溯证据；已收录报告不覆盖。

## 最新容量阶梯

| 目标速率 | 单轮结果 | 原始报告 |
| --- | --- | --- |
| 3000 req/s | HTTP、Realtime、Sync 完整，missing/duplicate/dead 为 0 | [`loadtest-rate-3000-db-acquire-r1.json`](./reports/loadtest-rate-3000-db-acquire-r1.json) |
| 3500 req/s | HTTP、Realtime、Sync 完整，missing/duplicate/dead 为 0 | [`loadtest-rate-3500-db-acquire-r1.json`](./reports/loadtest-rate-3500-db-acquire-r1.json) |
| 4000 req/s | HTTP、Realtime、Sync 完整，但准备 Lane 已接近预算 | [`loadtest-rate-4000-db-acquire-r1.json`](./reports/loadtest-rate-4000-db-acquire-r1.json) |
| 5000 req/s | 首个明确过载：HTTP 未全部启动，Realtime/Sync 未在核验窗口内完整；成功写入数据最终完整 | [`loadtest-rate-5000-db-acquire-r1.json`](./reports/loadtest-rate-5000-db-acquire-r1.json) |

3500 和 4000 均只是新鲜隔离库上的单轮证据，不等于稳定生产容量。当前可证实的区间是“4000 单轮完成，5000 明确过载”，精确且可重复的边界仍需在两者之间复测。

## 相关文档

- [`LOAD_TEST.md`](../LOAD_TEST.md)：方法、配置、A/B 结果与证据边界。
- [`OBSERVABILITY.md`](../OBSERVABILITY.md)：监控指标和排障顺序。
- [`TODO.md`](../TODO.md)：下一轮容量实验。
