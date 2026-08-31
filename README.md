# im-backend

一个用 Go、PostgreSQL 和 Redis 构建的即时消息后端，重点验证消息可靠投递、断线补拉、认证会话和可观测性，而不只是实现一个能发送消息的 HTTP 接口。

> 项目当前是可运行、可测试的学习与实验实现，不应直接作为生产系统部署。现有压测结论来自单机本地环境，不代表云上或多机生产容量。

## 核心能力

- 用户注册、Argon2id 密码哈希、短期 Access Token、Refresh Token 轮换及多设备 Session 管理。
- 消息和 Outbox 事件在同一 PostgreSQL 事务中提交，避免“消息写入成功但通知事件丢失”。
- Outbox Worker 使用短事务 claim、租约、重试、dead 状态和 `FOR UPDATE SKIP LOCKED`；连续运行时以有界两段流水线重叠下一批 prepare 与当前批 publish，网络发布不占用数据库行锁。
- 单聊双方规范化为一个 `conversation_id`；消息只写一份，并在会话内分配连续 `conversation_seq`。
- 新消息使用 Outbox payload v4：既有会话先走只读快路径；序号分配、消息写入和 ready Outbox 再由一条 data-modifying CTE 原子完成，不再为每个参与用户复制 `user_message_events`。
- 重连时先分页扫描 `GET /conversations`，再按每个会话的 cursor 调用 `GET /conversations/{id}/messages`；设备 ACK 也按会话保存。WebSocket 只负责实时通知，SQL 仍是恢复权威。
- v3 用户级 Sync projector、`user_sync_counters`、`user_message_events` 和相关实验模式暂时保留，只用于排空迁移前事件与短期回退；正常 v4 路径会跳过它们。
- 普通单聊历史使用绑定 `conversation_id + conversation_seq` 的 `before` / `after` opaque cursor 和单个会话索引，不通过 `OFFSET` 扫描深页。
- Redis 保存跨实例 WebSocket presence 并承载实时路由，本地 Hub 管理连接、背压和慢客户端断开。
- Outbox 投递前对同批 recipient 去重，用两段 Redis pipeline 生成仅在本批存活的 Presence 快照，避免热点用户被重复查询。
- Prometheus 指标覆盖 HTTP、连接池、Outbox 各阶段、Redis 路由、WebSocket、Sync 和 ACK。
- 自带真实链路压测器，分别校验 HTTP、Realtime 和 Sync durability，不把“HTTP 成功”误当成“消息已送达”。

## 消息主链路

```text
POST /messages
  -> PostgreSQL: resolve/create conversation（既有会话只读快路径）
     + allocate conversation_seq + message + ready v4 outbox（同一 SQL、同一事务）
  -> Outbox 准备 Lane: claim；v4 无用户级投影
  -> Outbox 投递 Lane: 批内 Presence 快照 + Redis / 本地 Hub + mark published
  -> WebSocket 携带 conversationId + conversationSeq 实时通知

断线或实时通知失败
  -> GET /conversations
  -> GET /conversations/{conversationId}/messages
  -> 客户端持久化后 POST /conversations/{conversationId}/ack
```

当前默认仍是 `OUTBOX_PREPARE_MODE=inline`、`OUTBOX_PREPARE_WORKERS=1`，但 v4 的 prepare 是无数据库写入的兼容步骤。`user_sharded`、projection mode 与 projection storage 开关只影响尚未排空的 v3 事件，不是新消息容量方案。

该模型把热点从“同一用户的全局 counter”缩小到“同一会话的 counter”：不同会话可以并行写，同一会话为保证严格顺序仍会串行更新一行。超热点群聊未来仍需单独评估序号分段或其他排序方案，当前单聊实现没有宣称消除所有热点。

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
| `GET` | `/messages` | 按 `peerId`、`before` / `after` cursor 和 `limit` 双向分页查询会话消息 |
| `GET` | `/conversations` | 按稳定 membership snapshot 分页列出当前用户的单聊会话与 `lastSeq` |
| `GET` | `/conversations/{conversationId}/messages` | 按该会话的 `after` / `snapshotCursor` 补拉消息 |
| `POST` | `/conversations/{conversationId}/ack` | 提交当前设备已持久化的会话 cursor |
| `GET` | `/messages/sync` | 旧用户级 Sync；返回 `410 Gone` 并提示迁移 |
| `POST` | `/messages/ack` | 旧用户级 ACK；返回 `410 Gone` 并提示迁移 |
| `GET` | `/ws` | 建立认证 WebSocket |

`GET /messages` 默认返回最新 50 条，响应是分页对象而不是裸数组：

```json
{
  "conversationId": 42,
  "messages": [],
  "beforeCursor": "opaque-cursor-for-the-oldest-returned-message",
  "afterCursor": "opaque-cursor-for-the-newest-returned-message",
  "hasMoreBefore": false,
  "hasMoreAfter": false
}
```

`before` 从当前页第一条继续向历史方向翻，`after` 从当前页最后一条向新消息方向翻；两者不能同时传入。`limit` 默认为 50，最大为 200。

断线恢复的会话消息页使用数字型会话 cursor：

```json
{
  "conversationId": 42,
  "messages": [],
  "nextCursor": 100,
  "snapshotCursor": 137,
  "hasMore": true
}
```

客户端应先建立 WebSocket，再扫描会话列表并逐会话补拉；扫描期间收到的新消息由 WebSocket 覆盖。连接中断时重新开始一轮会话扫描，避免把不完整的一轮当成恢复完成。

已有数据库升级到本模型时，先停止消息写入，再依次执行 `014`、`015`。`015` 会按规范化用户对回填会话和序号，并补全未发布 v1/v2/v3 Outbox payload；它是停写迁移，不是面向超大表的在线分批迁移。

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

`benchmarks/reports/` 中原有 3000～5000 req/s 容量阶梯来自 v3 用户级 cursor 投影架构，只能作为改造前基线。当前 v4 在每轮全新隔离状态下完成了发送事务 A/B：既有会话快路径加单 SQL 临界区后，10 会话 `ring` 3000 req/s 从 54552/60000 恢复到 60000/60000，HTTP P95 从 409.77ms 降到 16.73ms；单会话 `hot` 2000 req/s 仍失败，说明严格连续 `conversation_seq` 仍是热点边界。独立 Outbox Pool 可在过载时保护实时投递，但会增加 PostgreSQL 并发并回退普通链路延迟，所以默认继续共享 Pool，仅保留显式开关。精确条件、延迟和边界见 [LOAD_TEST.md](./LOAD_TEST.md)，这些本机短时结果不是生产容量承诺。

## 许可证

[MIT](./LICENSE)
