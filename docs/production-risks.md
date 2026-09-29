# Sự cố production thường gặp và cách xử lý

| # | Sự cố | Áp dụng cho |
|---|---|---|
| 1 | [WAL phình to, đầy disk](#1-wal-phình-to-đầy-disk) | Postgres |
| 2 | [Binlog đã bị purge](#2-binlog-đã-bị-purge) | MySQL |
| 3 | [Schema history topic bị hỏng hoặc mất dữ liệu](#3-schema-history-topic-bị-hỏng-hoặc-mất-dữ-liệu) | MySQL, SQL Server, Oracle… |
| 4 | [Duplicate event](#4-duplicate-event) | Tất cả |
| 5 | [Initial snapshot quá lâu, lock bảng](#5-initial-snapshot-quá-lâu-lock-bảng-ảnh-hưởng-db-chính) | Tất cả |
| 6 | [Thiếu `before` hoặc giá trị lạ trong event](#6-thiếu-before-hoặc-giá-trị-lạ-trong-event) | Postgres |
| 7 | [Thay đổi schema (DDL) làm vỡ pipeline](#7-thay-đổi-schema-ddl-làm-vỡ-pipeline) | Tất cả |
| 8 | [Message quá lớn](#8-message-quá-lớn) | Tất cả |
| 9 | [Lag tăng, throughput thấp](#9-lag-tăng-throughput-thấp) | Tất cả |
| 10 | [Failover database](#10-failover-database) | Postgres, MySQL |
| 11 | [Connector FAILED hoặc rebalance liên tục](#11-connector-failed-hoặc-rebalance-liên-tục) | Kafka Connect |
| 12 | [Sai lệch dữ liệu kiểu số và thời gian](#12-sai-lệch-dữ-liệu-kiểu-số-và-thời-gian) | Tất cả |

---

## 1. WAL phình to, đầy disk

> **Postgres** — sự cố số 1.
>
> ✅ Project này đã áp dụng heartbeat — xem [debezium-setup.md, Bước 3](debezium-setup.md#3-bảng-heartbeat-chống-wal-phình-to).

**Nguyên nhân** — ba tình huống điển hình:

- Connector dừng, hoặc bị xóa nhưng quên drop slot. Slot vẫn giữ WAL.
- Bảng được capture ít thay đổi, trong khi DB có nhiều bảng khác ghi liên tục. Connector không nhận event nào
  nên không commit LSN, và WAL tiếp tục bị giữ lại.
- Consumer chậm, tạo back-pressure lên connector.

**Xử lý**

- Bật `heartbeat.interval.ms` kèm `heartbeat.action.query`, ghi vào một bảng heartbeat nằm trong publication.
  Cách này giúp slot luôn tiến lên.
- Giám sát độ trễ của slot:

  ```sql
  SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
  FROM pg_replication_slots;
  ```

- Đặt `max_slot_wal_keep_size` (PG13+) để giới hạn WAL bị giữ.
  Đổi lại, slot có thể bị invalidate, và khi đó phải snapshot lại.
- Drop slot mồ côi:

  ```sql
  SELECT pg_drop_replication_slot('...');
  ```

---

## 2. Binlog đã bị purge

> **MySQL**

**Triệu chứng** — Connector báo lỗi kiểu *"binlog position is no longer available"* sau khi bị dừng quá lâu.

**Xử lý**

- Tăng `binlog_expire_logs_seconds`, tối thiểu phải dài hơn thời gian downtime tối đa có thể xảy ra.
  Trên RDS thì dùng `mysql.rds_set_configuration('binlog retention hours', …)`.
- Nếu đã mất binlog: bắt buộc snapshot lại (`snapshot.mode=when_needed`),
  hoặc dùng incremental snapshot cho các bảng bị ảnh hưởng.

---

## 3. Schema history topic bị hỏng hoặc mất dữ liệu

**Nguyên nhân** — Topic được tạo với retention mặc định (7 ngày) nên DDL cũ bị xóa, hoặc có người xóa nhầm topic.

**Phòng tránh** — Topic phải có:

| Cấu hình | Giá trị |
|---|---|
| Số partition | `1` |
| `retention.ms` | `-1` |
| `retention.bytes` | `-1` |
| `cleanup.policy` | `delete` (không được compact) |

**Khắc phục** — Chạy `snapshot.mode=recovery` (tên cũ là `schema_only_recovery`) để dựng lại history từ schema hiện tại.

> ⚠️ Chỉ an toàn nếu **không có DDL nào** xảy ra kể từ offset cuối.

---

## 4. Duplicate event

Đây là bản chất của **at-least-once**, và sẽ xảy ra khi restart, rebalance hoặc crash.

**Xử lý**

- Consumer phải **idempotent**: upsert theo PK.
- Dedup theo vị trí log trong source, hoặc dựa trên `version` / `updated_at`:
  - Postgres: `lsn`
  - MySQL: `file` + `pos`, hoặc GTID
- Nếu stack hỗ trợ, bật exactly-once source.

---

## 5. Initial snapshot quá lâu, lock bảng, ảnh hưởng DB chính

**Xử lý**

- Dùng `snapshot.mode=no_data`, sau đó chạy **incremental snapshot** qua signal.
  Cách này chạy theo chunk, không lock, và resume được sau khi restart.
- **MySQL:** đặt `snapshot.locking.mode=none` (cần chắc chắn không có DDL trong lúc snapshot), hoặc capture từ replica.
- **Postgres 16+:** hỗ trợ logical decoding trên standby, giúp giảm tải cho primary.

---

## 6. Thiếu `before` hoặc giá trị lạ trong event

> **Postgres**

**Triệu chứng**

- Update/delete chỉ có PK trong `before`. Nguyên nhân là replica identity mặc định chỉ ghi PK.
- Cột TOAST lớn (`text`, `jsonb`) không thay đổi sẽ xuất hiện dưới dạng placeholder `__debezium_unavailable_value`.

**Xử lý** — Chạy `REPLICA IDENTITY FULL` cho các bảng cần giá trị đầy đủ (lưu ý WAL sẽ tăng),
hoặc cho consumer merge với state đã có.

```sql
ALTER TABLE <table> REPLICA IDENTITY FULL;
```

---

## 7. Thay đổi schema (DDL) làm vỡ pipeline

**Triệu chứng** — Schema Registry từ chối schema mới vì incompatible (đổi kiểu cột, drop cột `NOT NULL`…),
hoặc sink table không tự migrate theo.

**Xử lý**

- Chọn compatibility mode phù hợp (thường là `BACKWARD`).
- Thực hiện DDL theo quy trình **expand/contract**: thêm cột nullable trước, drop sau.
- Phối hợp với team DB và có checklist DDL cho các bảng đang được CDC.
- Đổi PK sẽ sinh ra cặp event **delete + create**. Consumer cần xử lý được trường hợp này.

---

## 8. Message quá lớn

**Triệu chứng** — Lỗi `RecordTooLargeException` với row chứa blob hoặc JSON lớn.

**Xử lý**

- Tăng `producer.override.max.request.size` cùng `max.message.bytes` của topic (và cả phía broker).
- Bật compression.
- Hoặc loại bỏ các cột lớn bằng `column.exclude.list`.

> 💡 Để override producer theo từng connector, worker phải cấu hình
> `connector.client.config.override.policy=All`.

---

## 9. Lag tăng, throughput thấp

**Nguyên nhân thường gặp** — Chỉ có một task đọc log, batch nhỏ, producer không batch, hoặc Kafka chậm.

**Xử lý**

- Tăng `max.batch.size` và `max.queue.size`, bật `linger.ms` và compression.
- Tách bảng sang nhiều connector. Với Postgres, mỗi connector cần slot riêng, nên phải cân nhắc số slot.
- Lọc bớt bảng/cột không cần thiết.
- Giám sát `MilliSecondsBehindSource`. Nếu `QueueRemainingCapacity` về gần 0, nút thắt nằm ở phía Kafka hoặc producer.

---

## 10. Failover database

### Postgres

Replication slot mặc định **không** được replicate sang standby, nên sau failover slot bị mất.
Có thể mất event, và cần snapshot lại.

**Giải pháp**

- PG17: failover slots (`failover=true` kèm `sync_replication_slots`).
- Extension `pg_failover_slots`.
- Aurora và RDS có cơ chế riêng.

### MySQL

Bắt buộc phải bật **GTID** thì connector mới chuyển sang primary mới đúng vị trí.

---

## 11. Connector FAILED hoặc rebalance liên tục

**Task FAILED**

- Kiểm tra trạng thái: `GET /connectors/<name>/status`.
- Task FAILED **không tự restart**. Restart bằng:

  ```http
  POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true
  ```

**Rebalance liên tục**

- Dùng incremental cooperative rebalancing (mặc định từ Kafka 2.3+).
- Chỉnh `scheduled.rebalance.max.delay.ms`.
- Kiểm tra heartbeat hoặc GC của worker.

**Sửa hoặc reset offset**

- Kafka 3.6+: REST API `GET` / `PATCH` / `DELETE /connectors/<name>/offsets`. Connector phải được **stop** trước.
- Version cũ hơn: phải ghi tay vào offset topic.

---

## 12. Sai lệch dữ liệu kiểu số và thời gian

| Vấn đề | Xử lý |
|---|---|
| `DECIMAL` ra dạng bytes (base64) khiến consumer đọc sai | Đặt `decimal.handling.mode=string`, hoặc `double` nếu chấp nhận mất độ chính xác |
| Timestamp lệch timezone | Phân biệt `TIMESTAMP` và `TIMESTAMPTZ` / `DATETIME`, chỉnh `time.precision.mode`. Với MySQL, kiểm tra thêm `database.connectionTimeZone` |
