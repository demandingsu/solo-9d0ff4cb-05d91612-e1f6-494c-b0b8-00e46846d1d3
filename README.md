# 港航 AIS 靠港停留事件 API

岸基 AIS 点位 → 靠港停留事件，供排放核算使用。FastAPI + SQLite（WAL），
提供 Docker Compose 启动、CSV 上传、事件查询与游标变更流。

## 快速开始

```bash
docker compose up --build
```

* Swagger UI: <http://localhost:8000/docs>
* OpenAPI JSON: <http://localhost:8000/openapi.json>
* 健康检查: <http://localhost:8000/healthz>

演示迟到补点与撤回：在 `/docs` 中展开 **demo → POST /demo/run → Try it out →
Execute**。每次调用创建独立的 `DEMO-XXXXXXXX` 港区，不会污染业务数据。

演示按批次撤销：在 `/docs` 中展开 **demo → POST /demo/revoke → Try it out →
Execute**。展示重传来源保护、撤销后事件重算、重复撤销与已撤销批次重投。

双实例（共享同一 SQLite 卷，验证交错上传不丢更新）：

```bash
docker compose -f docker-compose.dual.yml up --build
# http://localhost:8001/docs 与 http://localhost:8002/docs
```

## 业务规则

| 规则 | 实现 |
| --- | --- |
| 固定港区矩形范围 | `POST /ports`，同一 `code` 重复登记内容一致时幂等（200），不一致 409 |
| 多次上传 CSV | `POST /ports/{code}/uploads`，含船舶号、UTC 时间、经纬度 |
| 同船同刻同坐标去重 | 批次内与库内均去重，计入 `rows_duplicate` |
| 同船同刻异坐标整批拒收 | 返回 409 `conflicting_coordinates`，整批回滚，不留半批结果 |
| 按时间重排 | 每船按 `(event_time, id)` 排序后推导事件 |
| 相邻在港点 ≤ 10 分钟 | 间隔 > 600 秒断开 |
| 首末相距 ≥ 30 分钟 | 不足 1800 秒不形成事件 |
| 出港点断开 | 矩形外的点作为退出标记，关闭当前事件且不并入 |
| 迟到点改变事件 | 未变事件保留 `event_id`；被改变事件旧号 retract + 新号 upsert |
| 游标顺序读取变更 | `GET /ports/{code}/changes?after=<seq>`，严格递增、可续读 |
| 上传原子生效 | 原始点、现役事件、变更记录同一 `BEGIN IMMEDIATE` 事务提交 |
| 按批次撤销 | `POST /ports/{code}/uploads/{batch_id}/revoke`，只删无其他有效来源的点 |
| 来源失效不误删重传 | 上传逐点记录 `point_sources`；同船同刻同坐标被其他有效批次重传时保留 |
| 缺少来源记录的旧批次 | `provenance_version=0`，撤销返回 409 `batch_provenance_incomplete` |
| 撤销后重算事件 | 仅对受影响的船按剩余点重算；未变事件保留编号，变化事件续发 retract + upsert |
| 重复撤销 | 返回同一撤销结果（replayed=true），不产生新变更 |
| 已撤销批次重投 | 同 `Idempotency-Key` + 同内容返回 `revoked=true`，不复活、不写点 |
| 撤销原子生效 | 来源、点位、事件、变更同一事务；校验/写入失败整体回滚 |
| 两实例交错上传不丢更新 | SQLite WAL + `BEGIN IMMEDIATE` 写串行化（上传与撤销互斥） |
| 失败不推进游标 | 事务回滚，`seq` 不前进，无半批结果 |
| 重复上传不重发变更 | `Idempotency-Key`（缺省用内容 SHA-256）唯一约束，重放返回原批次 |

### 事件编号语义

事件以 `(port, vessel, start_time)` 为身份。迟到点只延长结束时间或增加点数时，
仍会 **retract 旧编号 + upsert 新编号**，保证消费方总能拿到完整快照而无需
patch 既有编号；起点不变、载荷不变的事件保留原编号。CSV 中落在港区矩形外的
点位也会入库（作为出港标记），`rows_outside_port` 会统计它们。

### CSV 格式

```csv
vessel,time,lat,lon
SHIP-001,2026-01-01T00:00:00Z,31.20,121.50
SHIP-001,2026-01-01T00:10:00Z,31.201,121.501
```

表头别名（大小写不敏感）：`vessel/船舶号/mmsi/ship_id`、`time/时间/timestamp`、
`lat/纬度`、`lon/lng/longitude/经度`。时间支持 ISO-8601（无时区按 UTC）或
epoch 秒/毫秒。坐标按 1e-6 度取整后比较。上传上限默认 10 MiB / 100,000 行，
可用 `AIS_MAX_UPLOAD_BYTES`、`AIS_MAX_ROWS` 调整。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ports` | 登记港区矩形范围（幂等） |
| GET | `/ports/{code}` | 读取港区定义 |
| POST | `/ports/{code}/uploads` | 上传 CSV 点位，整批原子生效 |
| POST | `/ports/{code}/uploads/{batch_id}/revoke` | 按批次撤销上传（幂等、原子） |
| GET | `/ports/{code}/events` | 现役事件，按 `event_id` 分页 |
| GET | `/ports/{code}/changes` | 变更流，`after` 游标 + `limit` |
| POST | `/demo/run` | 迟到补点 / retract 演示（`AIS_DEMO_ENABLED=1`） |
| POST | `/demo/revoke` | 按批次撤销 / 重传来源保护演示（`AIS_DEMO_ENABLED=1`） |
| GET | `/healthz` | 健康检查 |

上传响应示例：

```json
{
  "replayed": false,
  "batch_id": 7,
  "rows_received": 15,
  "rows_new": 15,
  "rows_duplicate": 0,
  "rows_outside_port": 0,
  "event_changes": {"upserted": 3, "retracted": 0},
  "change_cursor": {"from": 12, "to": 14}
}
```

变更响应示例（`GET /ports/CNSHA-PD/changes?after=14`）：

```json
{
  "port_id": 1,
  "next_cursor": 16,
  "has_more": false,
  "changes": [
    {"cursor": 15, "type": "retract", "event_id": 41, "vessel_id": "SHIP-001", "...": "..."},
    {"cursor": 16, "type": "upsert", "event_id": 42, "event": {"event_id": 42, "...": "..."}}
  ]
}
```

`seq` 在全库单调递增，同一港区的游标可能不连续；按 `seq > after` 读取即可。
`GET /events` 返回的 `cursor` 与变更流同源，可用于「先快照、后增量」。

### 按批次撤销

```http
POST /ports/CNSHA-PD/uploads/7/revoke
```

```json
{
  "replayed": false,
  "status": "revoked",
  "batch_id": 7,
  "revoked": true,
  "revoked_at": "2026-01-02T03:04:05+00:00",
  "points_removed": 4,
  "points_retained": 4,
  "event_changes": {"upserted": 1, "retracted": 2},
  "change_cursor": {"from": 21, "to": 23}
}
```

* 上传会为每个观测记录来源（`point_sources`），包括命中去重的点。撤销只删除
  **没有任何其他有效批次来源**的点；被其他批次重传过的同船同刻同坐标观测保留。
* 撤销后只对受影响的船按剩余点重算事件：未变化事件保留 `event_id`，变化事件
  以旧号 `retract`、新号 `upsert` 续发到同一变更流，`operation="revoke"` 标明来源。
* 撤销在单个 `BEGIN IMMEDIATE` 事务内完成，与上传互斥；失败整体回滚（批次仍可
  再次撤销）。
* 重复撤销返回 200 与同一撤销结果（`replayed=true`），不产生新变更。
* 已撤销批次以相同 `Idempotency-Key` 重投：返回 200、`revoked=true`，不写点、
  不发变更、不复活。
* 升级前写入的批次没有逐点来源记录（`provenance_version=0`），撤销返回 409
  `batch_provenance_incomplete`；服务启动时会自动迁移表结构，旧数据保持只读安全。

## 本地开发与测试

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
AIS_DEMO_ENABLED=1 uvicorn app.main:app --reload
```

测试覆盖：10 分钟/30 分钟边界、出港断开、批内与库内去重、异坐标整批回滚、
迟到点 retract/upsert 与编号保留、两事件合并、幂等重放、游标分页、港区隔离、
双实例（共享 DB 的并发线程与两个 FastAPI 实例）交错上传、按批次撤销（共享来源
保留、旧批次拒撤、重复撤销、已撤销重投、撤销崩溃回滚、上传/撤销跨实例交错、
schema v1 自动迁移）。`docker build` 未能在本环境执行（无 Docker 守护进程），
Compose 文件仅做了 YAML 解析校验。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `AIS_DB_PATH` | `./data/ais.sqlite3` | SQLite 路径（容器内 `/data/ais.sqlite3`） |
| `AIS_DEMO_ENABLED` | `0`（镜像内为 `1`） | 是否注册 `/demo/run` |
| `AIS_MAX_UPLOAD_BYTES` | `10485760` | 单次上传字节上限 |
| `AIS_MAX_ROWS` | `100000` | 单次上传唯一行数上限 |
| `AIS_SQLITE_BUSY_TIMEOUT_MS` | `30000` | 写锁等待时间，超时返回 503 |

## 已知限制

* 变更流只保留写入的记录、不提供保留期清理；长期运行需自行归档。
* 事件推导按船全量重算该港区点位；单船超大数据量时应改为增量窗口。
* 撤销按点逐条检查其余来源；单批次点数很多时是 O(点数) 次查询，适合岸基日文件规模。
* 早期（无逐点来源记录的）批次不可撤销，只能保留或另建批次覆盖。
* SQLite 共享卷适用于单机多实例；跨主机需换成服务端数据库。
* 不做鉴权；生产部署应置于反向代理/网关之后。
