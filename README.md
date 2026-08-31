# im-backend

一个用 Go、PostgreSQL 和 Redis 构建的即时消息后端，重点验证消息可靠投递、断线补拉、认证会话和可观测性，而不只是实现一个能发送消息的 HTTP 接口。

> 项目当前是可运行、可测试的学习与实验实现，不应直接作为生产系统部署。现有压测结论来自单机本地环境，不代表云上或多机生产容量。

## 核心能力

- 用户注册、Argon2id 密码哈希、短期 Access Token、Refresh Token 轮换及多设备 Session 管理。
- 消息和 Outbox 事件在同一 PostgreSQL 事务中提交，避免“消息写入成功但通知事件丢失”。
- Outbox Worker 使用短事务 claim、租约、重试、dead 状态和 `FOR UPDATE SKIP LOCKED`；连续运行时以有界两段流水线重叠下一批 prepare 与当前批 publish，网络发布不占用数据库行锁。
- Sync cursor 与 Outbox ready 在同一事务提交；`user_message_events` 同时作为断线补拉和 Worker 崩溃恢复时的权威 user/cursor 数据源，正常路径不再重复写 `outbox_recipients`。
- 为每个用户分配连续 Sync cursor，支持快照分页补拉和设备 ACK；WebSocket 只负责实时通知，Sync API 负责恢复。
- Redis 保存跨实例 WebSocket presence 并承载实时路由，本地 Hub 管理连接、背压和慢客户端断开。
- Outbox 投递前对同批 recipient 去重，用两段 Redis pipeline 生成仅在本批存活的 Presence 快照，避免热点用户被重复查询。
- Prometheus 指标覆盖 HTTP、连接池、Outbox 各阶段、Redis 路由、WebSocket、Sync 和 ACK。
- 自带真实链路压测器，分别校验 HTTP、Realtime 和 Sync durability，不把“HTTP 成功”误当成“消息已送达”。

## 消息主链路

```text
POST /messages
  -> PostgreSQL: message + pending outbox（同一事务）
  -> Outbox 准备 Lane: claim + 分配用户 cursor
     + 写入 user_message_events + ready（同一事务）
  -> Outbox 投递 Lane: 批内 Presence 快照 + Redis / 本地 Hub + mark published
  -> WebSocket 实时通知

断线或实时通知失败
  -> GET /messages/sync
  -> 客户端持久化
  -> POST /messages/ack
```

## 本地运行

依赖：Go 1.26、Docker 和 Docker Compose。

1. 创建本地配置：

   ```bash
   cp .env.example .env
   openssl rand -base64 32 | tr -d '\n='
   ```

   把第二条命令生成的值填入 `.env` 的 `IDEMPOTENCY_KEY`。这个密钥用于加密幂等响应；已使用后不要随意更换，部署环境必须单独生成并安全保存。

2. 启动 PostgreSQL 和 Redis：

   ```bash
   docker compose up -d
   ```

   `schema.sql` 会在首次创建 PostgreSQL volume 时初始化完整 schema。已有数据库请按 `migrations/` 顺序管理变更，不要假设重启容器会重新执行 schema。

3. 加载环境变量并启动服务：

   ```bash
   set -a
   source .env
   set +a
   go run .
   ```

4. 验证：

   ```bash
   curl http://127.0.0.1:8080/health
   curl http://127.0.0.1:9090/metrics
   ```

默认 API 地址为 `:8080`，Prometheus 指标只监听 `127.0.0.1:9090`。`compose.yaml` 中的账号密码仅用于本地开发。

## API 概览

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `POST` | `/auth/register` | 注册用户 |
| `POST` | `/auth/login` | 登录并创建设备 Session |
| `POST` | `/auth/refresh` | 轮换 Token |
| `POST` | `/auth/logout` | 注销当前 Session |
| `GET` | `/auth/sessions` | 查看当前用户的 Session |
| `POST` | `/messages` | 幂等发送消息 |
| `GET` | `/messages` | 查询会话消息 |
| `GET` | `/messages/sync` | 按 cursor 补拉持久事件 |
| `POST` | `/messages/ack` | 提交设备已持久化 cursor |
| `GET` | `/ws` | 建立认证 WebSocket |

## 测试与压测

运行代码检查：

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

真实链路压测会写入数据库，必须显式提供 `-allow-write` 和允许的数据库名：

```bash
go run ./cmd/im-loadtest \
  -allow-write \
  -allow-database im \
  -report ./loadtest-report.json
```

压测方法、指标解释和实验边界见 [LOAD_TEST.md](./LOAD_TEST.md)；监控与排障顺序见 [OBSERVABILITY.md](./OBSERVABILITY.md)；待验证实验见 [TODO.md](./TODO.md)。已收录的脱敏原始报告和容量阶梯索引见 [benchmarks/README.md](./benchmarks/README.md)；项目根目录的默认临时输出 `loadtest-report*.json` 不进入 Git。

当前本机隔离库证据显示：在 Pool 24、Batch 64、10 个热点用户下，3000、3500 和 4000 req/s 都有单轮完整记录。切换为默认 `sync_events` 后的新鲜隔离库 5000 req/s 复核仅完成 99190/100000 HTTP，Realtime/Sync 在核验窗口内分别为 90884/396760 和 45492/198380，pending/oldest 峰值达到 `79891/45.861s`。API/Outbox acquisition P95 为 `250/100ms`，`prepare_store` 平均 95.23ms，而 `publish` 仅 1.78ms。流量停止后 Worker 用 10 秒排空，99190 条成功消息最终对应 198380 条 Sync 事件，pending/dead 为 0。这证明 5000 在规定窗口内明确过载，主压力在共享 PostgreSQL 连接获取和事务内写入，不是 Redis/Channel 发布；精确可重复边界仍需在 4000～5000 之间复测。完整方法和原始报告见 [LOAD_TEST.md](./LOAD_TEST.md)。

## 许可证

[MIT](./LICENSE)
