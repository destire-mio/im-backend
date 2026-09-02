# Redis、红包与租约故障实验

这是一套隔离的正确性实验，不是 IM 生产功能上线或容量结论。新增代码在 `internal/reliabilitylab/`，现有 Outbox 的真实进程验证在 `outbox_process_lab_test.go`。没有把 Redis 引入资金事实，也没有给 IM Worker 增加 Redis 锁。

## 当前基线与范围

- 工作树：`/Users/destire/.codex/worktrees/1da6/后端`，基于 `2165d27`。
- 同时只读检查了 `/Users/destire/Documents/ChatGPT/后端` 的未提交实现。那里已有 `redis_cache_lab_test.go`，Redis 是真实服务，但权威值使用 `atomic.Int64`，只证明指定调度下的回填竞态。这里补真实 SQL、进程与服务故障，不复制该实验。
- 主目录 Outbox 已修改，但 `claim` 更新 UUID `lock_token`、完成/失败回写校验 token 的关键 SQL 与本工作树一致。本轮直接运行本工作树的真实 Worker 方法；没有声称主目录整套未提交改动已回归。
- 未修改现有应用代码、根 `schema.sql`、迁移、依赖、Compose 或原有文档。没有处理容量、部署回滚或差距审计。首次实验交付时未提交或推送；后续发布状态以 Git 历史为准。
- 实验 DDL 不属于 `migrations/`：每个测试使用随机 PostgreSQL schema，结束时删除。脚本另建一次性 PostgreSQL 容器；只有 Outbox 测试在该容器的 `public` 中执行项目迁移 `001–015`。

## 运行

需要 Go 1.26、Docker、Unix 信号和本地 `redis-server`。脚本不会连接正在使用的 PostgreSQL/Redis，也不会清空共享数据库。

```bash
./scripts/reliability-lab.sh

# 明确指定另一个 Redis 二进制，不替换系统 Redis：
LAB_REDIS_SERVER=/absolute/path/to/redis-server ./scripts/reliability-lab.sh

# 仅筛选实验包中的用例；脚本随后仍会执行 Outbox 对照测试：
./scripts/reliability-lab.sh -run TestRedisSentinel
```

脚本固定创建独立 PostgreSQL 容器并绑定随机 loopback 端口；测试创建独立 Redis 主从/Sentinel 进程和临时持久化目录。退出时回收所属容器、进程和临时状态。测试失败会让脚本返回非零。

普通 `go test ./...` 会跳过这些真实故障实验。`RUN_RELIABILITY_LAB=1` 时必须提供 `LAB_DATABASE_URL`，缺失会失败；建议始终通过脚本使用一次性数据库。`TestLabProcessHelper` 的顶层 SKIP 是子进程入口，子进程会单独执行它，不代表故障用例被跳过。

## Redis：实测矩阵与一致性边界

| 测试 | 真实故障链与断言 |
| --- | --- |
| `TestCacheSQLStaleRefillAfterBothDeletes` | 旧读者从 PostgreSQL 读旧值后阻塞；SQL 提交新值；两次 DEL 都完成；旧读者再回填，Redis 真实出现旧值；TTL 到期后由 SQL 修复。证明延迟双删仍依赖读者完成时间。 |
| `TestCacheInvalidationSurvivesProcessAndRedisFailure` | 子进程原子提交 SQL 新值和失效意图后被 SIGKILL；Redis 被 SIGKILL，DEL 报错，SQL 失效意图保留；读操作回源 SQL；AOF 重启恢复旧缓存后重放 DEL、确认失效任务，再读到新值。 |
| `TestRedisExpirationAndEviction` | PTTL 为正到 GET nil，`expired_keys` 增加；`noeviction` 真实返回 OOM 且不淘汰；`allkeys-lru`、`volatile-lru` 真实增加 `evicted_keys`，后者保留无 TTL 的键。无 QPS 指标。 |
| `TestRedisPersistenceAfterSIGKILL` | RDB 手工 SAVE 后再写入，SIGKILL 重启只恢复快照内数据；AOF `appendfsync always` 重启恢复已确认写入；停机期间过期的键不复活。 |
| `TestRedisSentinelFailoverAndAcknowledgedLoss` | 一主一从、三个 Sentinel、quorum=2。先用同一物理连接 SET+WAIT 同步基准键；暂停从库并从主库切断复制 socket；主库确认新写入后被 SIGKILL；恢复从库，Sentinel 自动提升；已存在的 failover client 重新发现可写主库。基准键保留、未复制写入丢失；旧主重启后被重配为从库，拒绝写入并同步新数据。 |

`cache.go` 的写路径使用一个 SQL 语句原子写入事实和失效意图；DEL 成功后才确认对应版本的任务。确认带版本条件，避免清掉并发产生的新任务；Redis 错误时读操作可回源 SQL，缓存写失败不撤销权威读取。

这是最终一致性实验。TTL 从缓存回填开始计时，不能在读者可能无限期挂起时承诺“SQL 提交后固定 N 秒必定无旧值”。此处失效重放由测试驱动，没有接入 IM 生产调度器；需要强一致的资金判定直接读 SQL。

首次 Sentinel 实验只执行 SIGSTOP，写入仍可能留在内核 socket 缓冲区，从库恢复后继续应用，因而“必定丢失”断言失败。最终版本明确断开复制连接，并断言 `connected_slaves=0` 后再写入；没有用 mock、删键或手工修改复制状态制造通过结果。

## 红包：HTTP → SQL → 持久化结果 → 可重放通知

`PacketStore.Create` 使用整数最小货币单位，按商与余数预分配所有份额。`10003 / 100` 产生 3 份 101、97 份 100。延迟 SQL 约束在事务提交时检查份数、连续槽位和金额总和；分配标识与金额不可更新。

`PacketHandler` 提供隔离的 `POST /packets/{packet}/claims`。用户身份由传入的认证回调提供；请求体不能指定领取人。实验回调只识别固定测试凭证，没有接入生产认证，也没有注册到 IM 的 routes/OpenAPI。

领取事务按顺序执行：

1. 锁定该红包行，使同一红包的短事务串行，不阻塞其他红包的独立行。
2. 先读该用户的已有结果；命中则返回原份额，即使红包已经领完。
3. 领取一个未使用槽位，写入资金领取记录和通知 Outbox，然后一起提交。
4. 客户端收到不确定结果时，使用同一红包和同一认证用户重试，从 SQL 查明结果。

SQL 唯一约束保护 `(packet_id, user_id)` 与 `(packet_id, slot)`；资金记录通过复合外键对应槽位、领取人和金额。Redis 不参与份数、余额或唯一性判断。单红包行锁是明确的正确性取舍，未宣称满足任意热点吞吐。

| 测试 | 已核验结果 |
| --- | --- |
| `TestPacketConcurrentClaims` | 400 个并发真实 HTTP 请求、200 个认证测试用户、100 份；100 个不同领取人，200 次原请求/幂等重试成功，200 次已领完；SQL 金额和 10003，通知 Outbox 恰好 100 条。 |
| `TestPacketSQLFailureAndConstraints` | 在资金记录 INSERT 处由 PostgreSQL trigger 抛错；槽位占用、资金记录和 Outbox 全部回滚，解除故障后重试成功；数据库直接拒绝错误预分配金额和重复领取人。 |
| `TestPacketProcessCrashRecovery` | 分别在占用槽位后、提交前、提交后、下游提交后 ACK 前 SIGKILL。重试前核对实际持久状态；新 OS 进程重试后只有一份领取结果和一次下游效果，Outbox 排空。 |
| `TestPacketHTTPResponseLostAfterCommit` | 真实 HTTP 已进入 SQL COMMIT，服务在返回响应前被 SIGKILL；客户端收到传输错误；新服务进程上的两次重试返回完全相同的原领取结果。无凭证请求被 401 拒绝。 |

补偿边界：SQL 未提交时靠事务回滚，不生成 Redis 预占，因此没有“Redis 扣减成功但 SQL 失败”的反向加回。SQL 已提交时不能因客户端超时退款，而应返回原结果。通知消费者使用独立提交的持久唯一键去重，再 ACK Outbox；下游已提交、ACK 丢失时安全重放。

`received_credits` 是同一个 PostgreSQL 实例上独立事务的持久消费者替身，证明本地去重/ACK 边界，不是外部支付系统的 exactly-once。这里建模的是已经提供总额的红包，没有实现或验证发送者钱包扣款、退款、红包到期取消、跨库清算、真实支付服务补偿或产品级公平随机分配。

## 分布式锁：过期后恢复的旧 owner

`TestLeaseExpiredOwnerRevives` 启动两个真实 Go 子进程，A 执行 SET NX PX 后被 SIGSTOP；父进程读取服务状态等待真实 TTL 到期；B 领取并写入；随后 SIGCONT A。

- 无 fencing：A 覆盖 B 的资源值，即使 A 使用 compare-and-delete、没有误删 B 的锁，业务写仍然越权。
- 有 SQL 条件写：领取时在 `resource_leases` 持久递增 token；资源写锁定同一租约行，并在写入时重新检查当前 token 和有效期。A 被拒绝，B 的值和 Redis 锁保留。Redis 不分配 token，重启 Redis 不会让 SQL 代际归零。

该实现只保护同一个 SQL 权威存储中的资源。租约行不得删除重建；不能把这个 token 自动当成远端 HTTP、文件写或任意第三方服务的防护。这里的 Redis 锁只用于对照实验，有 SQL claim 时不必保留冗余 Redis 锁。

`TestOutboxRealProcessExpiredOwner` 直接调用当前项目已有的 `worker.claim / markPublished / markPublishedBatch / markFailed`，不是复制 SQL 的模型。A SIGSTOP 到真实到期，B 获取新 UUID 但尚未完成时恢复 A；A 的单条完成、批量完成、重试及置死写全部返回 `errOutboxLeaseLost`，B 随后成功完成。另有既有双 Worker 不重复领取、旧 token 拒绝测试作为对照。

现有随机 UUID `lock_token` 是“当前代际必须相等”的数据库条件写，不是能跨系统排序的单调 fencing token。它足以保护这张表的状态转换，但不能撤回已发出的 WebSocket/网络副作用；投递仍需要重复容忍与 SQL/Sync 恢复。

## 证据与未验证项

原始日志保存在 `experiments/reliability/`。测试失败的首轮不作为通过证据；最终日志记录运行版本、每条故障链和 PASS。日志中的 connection refused 是杀 Redis 后的预期故障。

- [Redis 8.6.1 完整日志](experiments/reliability/2026-09-02-native-redis8.txt)：PostgreSQL 17.11，实验包与 Outbox 对照均启用 `-race`。
- [Redis 7.4.11 完整日志](experiments/reliability/2026-09-02-native-redis7.txt)：同一套测试再次全部通过。版本与现有 Compose 服务一致；使用 [Redis 官方 7.4.11 源码包](https://download.redis.io/releases/redis-7.4.11.tar.gz) 在临时目录编译的 macOS 原生二进制，`MALLOC=libc BUILD_TLS=no`，没有替换现有 Linux/jemalloc 服务。因此这不是对生产操作系统、分配器或部署配置的完全复刻。
- 全仓 `go test -count=1 ./...`、`go vet ./...`、`go build ./...` 通过；前者未配置普通集成测试环境，因此不能视为全仓真实 PostgreSQL/Redis 回归。
- [源码与运行日志校验和](experiments/reliability/SHA256SUMS) 固定本次未提交文件的内容；[运行环境说明](experiments/reliability/environment.txt) 记录基线、工具版本和 Redis 7 源码包校验和。

仍未验证：宿主机掉电/磁盘损坏、AOF everysec 的断电丢失窗口、AOF 截断与修复、RDB 后台快照失败、Redis Cluster、跨机器时钟/网络分区、多副本选择、Sentinel 丧失多数派、生产流量切换，以及旧主刚重启到完成重配之间的可写窗口。SIGKILL 是进程故障，不等于掉电；旧主最终 READONLY 不等于所有脑裂场景已被阻止。

这些结果支持“指定隔离实验已通过”，不支持“Redis/红包/分布式锁专题全面完成”或生产可靠性、容量与学习掌握程度的结论。

## 机制来源

- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)：RDB 与 AOF 恢复边界。
- [Redis replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)：复制异步，WAIT 不等于强一致系统。
- [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/)：选主与已确认写入丢失边界。
- [Distributed Locks with Redis](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)：租约、正确释放与 fencing 的不同职责。

来源解释机制；上面的通过结论来自本次实际运行，不以文档说明替代实验。
