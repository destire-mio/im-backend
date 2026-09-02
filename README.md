所有的文档以及代码100%是由ai完成 想了解我的学习记录会有一个文件夹 （todo中 里面的内容100%人写的）
# im-backend

一个用 Go、PostgreSQL 和 Redis 构建的即时消息后端，重点验证消息可靠投递、断线补拉、认证会话和可观测性，而不只是实现一个能发送消息的 HTTP 接口。

> 项目当前是可运行、可测试的学习与实验实现，不应直接作为生产系统部署。现有压测结论来自单机本地环境，不代表云上或多机生产容量。

## 核心能力

- 用户注册、Argon2id 密码哈希、短期 Access Token、Refresh Token 轮换及多设备 Session 管理；Session 同时受 90 天空闲期和自登录起 365 天绝对期限约束。
- 消息和 Outbox 事件在同一 PostgreSQL 事务中提交，避免“消息写入成功但通知事件丢失”。
- Outbox Worker 使用短事务 claim、租约、重试、dead 状态和 `FOR UPDATE SKIP LOCKED`；连续运行时以有界两段流水线重叠下一批 claim 与当前批 publish，网络发布不占用数据库行锁。
- 单聊双方规范化为一个 `conversation_id`；消息只写一份，并在会话内分配连续 `conversation_seq`。
- 新消息只写 ready v4 Outbox：会话序号、消息、投递对象和 ready 状态在同一事务中提交，不再生成旧用户级 Sync 记录。
- 重连时先分页扫描 `GET /conversations`，再按每个会话的 cursor 调用 `GET /conversations/{id}/messages`；设备 ACK 也按会话保存。WebSocket 只负责实时通知，SQL 仍是恢复权威。
- `016` 迁移结束旧二进制回滚窗口：一次性转换待投递旧事件、收紧会话字段约束并删除旧投影表。Worker 只处理 v4 消息事件。
- 普通单聊历史使用绑定 `conversation_id + conversation_seq` 的 `before` / `after` opaque cursor 和单个会话索引，不通过 `OFFSET` 扫描深页。
- Redis 保存跨实例 WebSocket presence 并承载实时路由，本地 Hub 管理连接、背压和慢客户端断开。
- Outbox 投递前对同批 recipient 去重，用两段 Redis pipeline 生成仅在本批存活的 Presence 快照，避免热点用户被重复查询。
- 认证限流在一个 Redis 脚本内先检查所有维度，再统一计数；被局部规则拒绝的流量不消耗全局准入额度。
- Prometheus 指标覆盖 HTTP、连接池、Outbox 各阶段、Redis 路由、WebSocket、Sync 和 ACK。
- 自带真实链路压测器，分别校验 HTTP、Realtime 和 Sync durability，不把“HTTP 成功”误当成“消息已送达”。

## 消息主链路

```text
POST /messages
  -> PostgreSQL: resolve/create conversation（既有会话只读快路径）
     + allocate conversation_seq + message + ready v4 outbox（同一 SQL、同一事务）
  -> Outbox claim（与上一批投递重叠）
  -> 批内 Presence 快照 + Redis / 本地 Hub + mark published
  -> WebSocket 携带 conversationId + conversationSeq 实时通知

断线或实时通知失败
  -> GET /conversations
  -> GET /conversations/{conversationId}/messages
  -> 客户端持久化后 POST /conversations/{conversationId}/ack
```

运行时只保留这一条链路。`OUTBOX_EXECUTION_MODE`、`OUTBOX_PREPARE_MODE`、`OUTBOX_PREPARE_WORKERS`、`OUTBOX_PROJECTION_MODE`、`OUTBOX_PROJECTION_STORAGE` 和 `OUTBOX_BATCH_PRESENCE_LOOKUP` 已移除，请从部署配置中删除；不再提供旧算法回退。保留 `OUTBOX_BATCH_SIZE` / `OUTBOX_CONCURRENCY` 这类资源参数。

该模型把热点从“同一用户的全局 counter”缩小到“同一会话的 counter”：不同会话可以并行写，同一会话为保证严格顺序仍会串行更新一行。超热点群聊未来仍需单独评估序号分段或其他排序方案，当前单聊实现没有宣称消除所有热点。

## 本地运行

依赖：Go 1.26、Docker 和 Docker Compose。

1. 创建本地配置：

   ```bash
   cp .env.example .env
   openssl rand -base64 32 | tr -d '\n='
   ```

   把第二条命令生成的值填入 `.env` 的 `IDEMPOTENCY_KEY`。这个密钥用于加密幂等响应，部署环境必须单独生成并安全保存。轮换时先提高 `IDEMPOTENCY_KEY_VERSION` 并把新密钥设为活动密钥，同时通过 `IDEMPOTENCY_PREVIOUS_KEYS=旧版本:旧密钥` 保留旧密钥至少 10 分钟；新记录只使用活动密钥，旧恢复记录按数据库中的 `key_version` 选择旧密钥解密。确认旧记录全部过期后才能删除旧密钥。

2. 启动 PostgreSQL 和 Redis：

   ```bash
   docker compose up -d
   ```

   `schema.sql` 会在首次创建 PostgreSQL volume 时初始化完整的 `016` 结构，但不写迁移历史。首次使用这种新 volume 后，显式登记当前基线：

   ```bash
   DATABASE_URL='postgres://...' go run ./cmd/im-migrate baseline -to 16
   ```

   对真正没有业务表的新建空数据库，使用正式迁移器从 `001` 安装到 `016`：

   ```bash
   DATABASE_URL='postgres://...' go run ./cmd/im-migrate up
   ```

   执行器会获取 PostgreSQL advisory lock，核对 `schema_migrations` 中的版本、名称和 checksum，
   并将普通迁移与其历史记录放在同一事务中。已有业务表但没有迁移历史的旧库会被拒绝，
   不会被自动当成空库。

   如果已有数据库是 `015` 的完整结构但没有迁移历史，先备份、停止 API 写入和所有 Outbox Worker，再显式接管并升级：

   ```bash
   DATABASE_URL='postgres://...' go run ./cmd/im-migrate baseline -to 15
   DATABASE_URL='postgres://...' go run ./cmd/im-migrate up -allow-maintenance
   ```

   `baseline` 只接受已核验的 `015` / `016` 语义指纹，比较表、字段、默认值、可空性、约束、索引、序列和统计对象。
   只有完全匹配才会在一个事务中补写对应版本的历史；它不重放 SQL，也不改业务数据。
   空库、结构漂移或已有迁移历史时都会拒绝。不要假设重启容器会重新执行 schema。

   应用实例本身不会执行迁移；它在连接 PostgreSQL 后会只读核对全部版本、名称和 checksum。
   数据库缺版本、比当前二进制更新、历史不连续或 checksum 不匹配时，实例会在启动 Worker 和 HTTP 服务前退出。
   部署顺序因此是：先单独运行 `im-migrate up`，成功后再启动或更新 IM 实例。

   `015` 负责初次回填会话；`016_contract_conversation_sync` 结束回滚兼容。`016` 会补齐回滚期间写入的缺失会话序号，把 pending v1/v2/v3 事件按消息表转换成 ready v4，并保留事件 ID、重试次数、退避时间和 Trace Context。published/dead 旧事件保留原样作为历史记录，不自动重新投递。

   `016` 随后将消息的会话字段设为 NOT NULL，禁止新增非 v4 或未 ready 的待投递消息事件，删除 `user_sync_counters`、`user_message_events`、`device_sync_states`、`outbox_recipients`、`message_projection_jobs` 及旧索引。当前会话 ACK 状态和消息正文保留。部分缺失的 cursor 或未知 pending payload 版本会使整次迁移回滚，需人工确认数据后重试。

   有历史消息时，`015` / `016` 都要求 `-allow-maintenance`。升级顺序固定为：

   ```text
   备份 -> 停全部 API / Outbox Worker -> im-migrate up -allow-maintenance -> 启动支持 016 的实例
   ```

   不允许新旧写实例混跑，也不能在迁移后直接回退到旧二进制。`reconcile-conversations` 命令已经删除，修复只发生在这次迁移中。应用回退必须仍兼容 `016`；恢复迁移前备份则需要另行处理切换后新增的数据。

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

`internal/headlessclient` 提供仅用于故障验证的最小 Headless 客户端。它先持久化 Refresh 幂等操作，再发网络请求；消息和会话 cursor 使用加密原子文件一起提交；WebSocket 序号出现缺口时保持原 cursor 并转 SQL Sync；本地提交后才发送累计 ACK。集成测试覆盖 Refresh 与 ACK 成功但响应丢失、进程重建、WebSocket 乱序、Sync 去重和多设备进度隔离。加密密钥由外部注入，磁盘文件不含明文 Token；这验证了状态机和密钥边界，但不冒充 Android Room/Keystore 的平台实现。

已有数据库必须通过 `im-migrate` 顺序升级到 `016`。历史回填和 contract 都是停写事务，不是面向超大表的在线分批迁移；部署前应在备份副本评估维护窗口。

## 测试与压测

当前 HTTP 合同从 [`openapi.yaml`](./openapi.yaml) 开始维护，已覆盖当前全部对外路由：认证与 Session、消息创建与历史分页、会话恢复与 ACK、废弃的用户级 Sync/ACK、health 和 WebSocket HTTP 握手。认证合同包含敏感令牌响应、输入错误、凭证失败、幂等 key 冲突与恢复、限流、依赖故障和内部错误；
`message_contract_test.go` 使用测试依赖 `kin-openapi` 对真实 Handler、认证中间件和
PostgreSQL 集成响应进行校验，包括成功响应与 400、401、404、409、410、426、429、500、503。WebSocket 升级后的实时消息协议仍由 WebSocket 集成测试负责，不把 OpenAPI 当成帧协议规范。
OpenAPI 合同测试负责请求/响应形状，消息与 Outbox 的事务原子性仍由 PostgreSQL 集成测试负责。

GitHub Actions 门禁位于 [`.github/workflows/ci.yml`](./.github/workflows/ci.yml)，使用独立的 PostgreSQL 应用测试库和破坏性迁移测试库，并启动 Redis。它会检查 gofmt，执行两次幂等 `im-migrate up`、全仓 race、`go vet` 和全包构建。客户端恢复与认证生命周期提交已在远程 `main@1e7606d` 的 GitHub Actions 运行 `33510855000` 通过全部门禁。

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

`benchmarks/reports/` 和 [LOAD_TEST.md](./LOAD_TEST.md) 中已有容量/A/B 报告是改造前的历史证据，不能作为 `016` 纯 v4 链路的新容量结论。本轮功能与故障测试不等于容量压测；需要重新建立相同条件下的阶梯基线。同一会话的连续序号仍是串行化边界，独立 Outbox Pool 默认关闭。

## 许可证

[MIT](./LICENSE)
