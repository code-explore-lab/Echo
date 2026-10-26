# MiniDanmaku

一个从零构建的弹幕（bullet-comment）系统，Phase 1 聚焦**单机链路做对**：多房间、实时广播、历史查询，并为多实例/节点故障预留扩展点。

技术栈：Go 1.22+ · `gorilla/websocket` · 标准库 `net/http` · MongoDB（mongo-driver v1）· `golang.org/x/time`。

## 快速开始

```bash
# 1. 启动 MongoDB（本地默认 mongodb://localhost:27017）
docker run -d -p 27017:27017 --name mongo mongo

# 2. 启动服务
go run ./cmd/server

# 3. 浏览器访问 http://localhost:8080/ 打开演示页（两个标签页可互发弹幕）
```

> 说明：演示页已用 `go:embed` 打进二进制，`/` 直接返回它；也可以直接双击打开本地 `web/index.html`（WS 地址自动指向当前 host）。

### 压测客户端

```bash
# 50 个并发连接，每连接发 200 条
go run ./cmd/client -c 50 -n 200 -room 100
```

## 架构

```
 web/index.html / cmd/client
        │  WebSocket (JSON)
        ▼
 gateway (net/http + gorilla)
   ├─ 鉴权（静态 token，可为空）
   ├─ 限流（每连接令牌桶）
   └─ 消息校验
        │
        ▼
 hub（内存房间 + 每连接双 goroutine）
   ├─ InMemoryRoom：成员表、去重缓存、房间内单调 seq
   │
   ├─ 实时路径：非阻塞 fan-out 到成员的有界 send 缓冲（可丢）
   └─ 存储路径：Enqueue 进有界落库队列（可丢、计数、绝不假装落库）
        │
        ▼
 store（PersistQueue → N 个 writer goroutine → MongoDB InsertOne）
```

**两条路径分离**：实时展示要求低延迟、允许丢；可靠存储要求重试、幂等，但 Phase 1 是「尽力而为」——`ack` 只代表「已受理并入队」，**不代表已落库**。

## 消息协议（JSON over WebSocket）

**Client → Server**

```json
{ "type": "danmaku", "id": "<客户端UUID>", "content": "你好", "color": "#ffffff", "size": 24 }
{ "type": "ping" }
```

**Server → Client**

```json
{ "type": "join_ok", "room": "100", "client": "<conn-id>", "seq": 0 }
{ "type": "danmaku", "id": "<msgId>", "room": "100", "uid": "u1", "content": "你好", "color": "#ffffff", "size": 24, "ts": 1730000000000, "seq": 42 }
{ "type": "ack", "id": "<客户端UUID>", "msgId": "<msgId>", "ok": true }
{ "type": "error", "code": "RATE_LIMITED|INVALID|AUTH_FAILED", "msg": "..." }
```

- `id` 是客户端幂等键：超时重发带同一个 `id`，服务端去重后返回**同一个** `msgId`，不重复广播/落库。
- `msgId` 由服务端生成，同时用作 MongoDB `_id`，重试插入天然幂等。
- 历史查询走 REST：`GET /history?room=100&from=<ts>&limit=50`（`from` 为分页游标，取 `ts < from` 的最新 `limit` 条）。

## 五个工程问题怎么解的

| 问题                   | 方案                                                                                                        |
| ---------------------- | ----------------------------------------------------------------------------------------------------------- |
| 每连接独立写协程       | 每个 `hub.Client` 只有 `writePump` 一个 goroutine 调 `WriteMessage`，`readPump` 只读。                      |
| 有界 channel、满时策略 | `send` 有界缓冲，满则「丢最新 + 计数」，累计丢弃超阈值就断开慢消费者让它重连。                              |
| 断开/心跳/重复/清理    | WS ping/pong + 读超时；`sync.Once` 保证只关一次；房间级有界 LRU 去重缓存；连接关闭后两个 goroutine 都退出。 |
| DB 写不阻塞广播        | 广播完成后再入有界落库队列（非阻塞），由 N 个 writer goroutine 异步写 Mongo。                               |
| Mongo 索引             | `{room:1, ts:-1, _id:-1}`（按房间按时间 + keyset 分页）；`{room:1, seq:1}` 唯一索引（重连补拉）。           |

## 多实例扩展点

只抽一处 `hub.Room` 接口（`Join` / `Leave` / `Broadcast` / `Ingest` / `Seq`）。Phase 1 实现是 `InMemoryRoom`；Phase 2 可用 Redis pub/sub 实现同一个接口（广播改 pub/sub，`Ingest` 改 `SETNX`/`INCR`），网关与客户端无需改动。

## 配置（环境变量，均有默认值）

| 变量                                       | 默认                        | 说明                       |
| ------------------------------------------ | --------------------------- | -------------------------- |
| `ADDR`                                     | `:8080`                     | 监听地址                   |
| `MONGO_URI`                                | `mongodb://localhost:27017` |                            |
| `MONGO_DB`                                 | `minidanmaku`               |                            |
| `AUTH_TOKEN`                               | 空（关闭鉴权）              | 静态共享 token             |
| `CLIENT_SEND_BUF`                          | `256`                       | 每连接出站缓冲             |
| `STORE_QUEUE`                              | `1024`                      | 落库队列容量               |
| `STORE_WORKERS`                            | `4`                         | 落库 writer 数量           |
| `RATE_LIMIT` / `RATE_BURST`                | `10` / `20`                 | 每连接限流（msg/s 与突发） |
| `PING_PERIOD` / `PONG_WAIT` / `WRITE_WAIT` | `30s` / `60s` / `10s`       | 心跳与写超时               |
| `MAX_MSG_SIZE`                             | `4096`                      | 单帧上限（字节）           |
| `DEDUP_SIZE` / `DEDUP_TTL`                 | `10000` / `60s`             | 去重缓存容量与窗口         |
| `MAX_DROPS`                                | `16`                        | 断开慢消费者的丢弃阈值     |

## 目录结构

```
cmd/server   服务入口（装配 + 优雅退出）
cmd/client   压测 CLI
internal/config   配置
internal/model    弹幕领域类型 + 校验 + ID 生成
internal/protocol JSON 协议
internal/hub      房间 / 客户端 / 双 pump / 去重
internal/gateway  HTTP 路由 / 鉴权 / WS 升级 / 历史查询
internal/store    落库队列 + MongoDB 实现 + 索引
web/index.html    浏览器演示页
```

## 验证

```bash
go build ./...
go vet ./...
go test ./...                          # 单元测试（离线，无需 Mongo）

# Mongo 集成测试（需本地 Mongo）
MINIDANMAKU_MONGO_URI=mongodb://localhost:27017 go test ./internal/store/ -run Integration -v
```

## 已知取舍（Phase 1）

- 关闭鉴权时允许任意来源的 WS 升级（`CheckOrigin` 恒真），暴露公网前需收紧。
- 优雅退出会关闭监听器并排空落库队列，但 WS 连接随进程退出而断开（未做逐连接的显式优雅关闭）。
- 历史查询分页以 `ts` 为游标，同一毫秒的边界消息在极端情况下可能重复/漏取（`_id` 已参与排序保证稳定；Phase 2 可加复合游标）。
