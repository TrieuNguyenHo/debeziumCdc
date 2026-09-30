# Debezium Postgres CDC — Setup & Troubleshooting

Debezium chạy **embedded trong app Spring Boot** qua Spring Integration Debezium (không dùng Kafka Connect).

```
Postgres (order_db.public.order_entity)
   │  logical replication (pgoutput, slot cdc_order_slot)
   ▼
┌──────────────────────── cdc-app (Spring Boot) ────────────────────────┐
│ DebeziumMessageProducer (Debezium embedded engine 3.5)                │
│   │  IntegrationFlow debeziumToKafkaFlow                              │
│   ▼                                                                   │
│ KafkaTemplate ──► topic cdc.order.public.order_entity (JSON, unwrap)  │
│                                                                       │
│ OrderCdcListener ◄── @KafkaListener cdc.order.public.order_entity     │
└───────────────────────────────────────────────────────────────────────┘
   offsets của engine: topic cdc-app.debezium-offsets
```

## Môi trường có sẵn (project saga-axon-kafka)

| Thành phần | Container | Trong network docker | Từ host |
|---|---|---|---|
| Kafka (cp-kafka 7.6.0) | `saga-axon-kafka-kafka-1` | `kafka:29092` | `localhost:9192` (xem Issue 4) |
| Postgres 16 | `saga-axon-kafka-postgres-1` | `postgres:5432` | `localhost:5432` |
| Kafka UI | `saga-axon-kafka-kafka-ui-1` | — | `http://localhost:9094` |

- Network: `saga-axon-kafka_default` (container `cdc-app` join vào dưới dạng `external`).
- Postgres user `saga` / `saga`, đã có quyền `REPLICATION`.
- Chỉ capture `order_db.public.order_entity`.

## Thành phần code

| File | Vai trò |
|---|---|
| `pom.xml` | `spring-boot-starter-integration`, `spring-integration-debezium` (7.1.1 → debezium-embedded 3.5.2.Final), `debezium-connector-postgres` `${debezium.version}`, `spring-boot-starter-kafka` |
| `application.yml` → `cdc.debezium.*` | Toàn bộ property của Debezium engine, truyền nguyên văn vào engine |
| `debezium/CdcProperties.java` | Bind `cdc.debezium` thành `java.util.Properties` |
| `debezium/DebeziumConfig.java` | `IntegrationFlow`: `Debezium.inboundChannelAdapter(...)` → `KafkaTemplate.send(destination, key, payload).join()`; `NewTopic` cho topic order |
| `order/OrderCdcListener.java`, `order/OrderChangeEvent.java` | Consumer log sự kiện |

> ⚠️ `debezium.version` trong `pom.xml` **phải khớp** version `debezium-embedded` mà `spring-integration-debezium`
> kéo về. Khi nâng Spring Boot, kiểm tra lại bằng:
> `./mvnw dependency:tree -Dincludes=io.debezium`

## Các bước setup

### 1. Bật logical decoding cho Postgres

Debezium yêu cầu `wal_level = logical` (mặc định là `replica`).

```sh
docker exec saga-axon-kafka-postgres-1 psql -U saga -d postgres -c "ALTER SYSTEM SET wal_level = logical"
docker restart saga-axon-kafka-postgres-1
docker exec saga-axon-kafka-postgres-1 psql -U saga -d postgres -Atc "show wal_level"   # -> logical
```

> `ALTER SYSTEM` ghi vào `postgresql.auto.conf` trong data volume → giữ nguyên qua restart,
> nhưng **mất nếu xoá volume**. Cách bền hơn: thêm `command: postgres -c wal_level=logical`
> vào compose của saga.

### 2. Publication & replication slot

`pgoutput` chỉ stream những bảng nằm trong một **publication**, đọc qua một **replication slot**.
Cả hai đều do Debezium **tự tạo** lúc engine khởi động, không cần chạy tay:

| Đối tượng | Tên | Cấu hình quyết định |
|---|---|---|
| Publication | `cdc_order_publication` | `publication.name` + `publication.autocreate.mode: filtered` |
| Replication slot | `cdc_order_slot` | `slot.name` |

Với `filtered`, Debezium chạy tương đương:

```sql
-- lần đầu (publication chưa tồn tại)
CREATE PUBLICATION cdc_order_publication FOR TABLE public.order_entity;
-- các lần khởi động sau: đồng bộ lại theo table.include.list
ALTER PUBLICATION cdc_order_publication SET TABLE public.order_entity;
```

Các giá trị của `publication.autocreate.mode`:
- `filtered` *(đang dùng)*: chỉ gồm các bảng trong `table.include.list`.
- `all_tables`: `CREATE PUBLICATION ... FOR ALL TABLES`, cần quyền superuser.
- `disabled`: Debezium không tạo; publication phải có sẵn, nếu không engine lỗi khi start.

Kiểm tra sau khi app chạy:

```sh
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db \
  -c "select pubname, puballtables, pubinsert, pubupdate, pubdelete from pg_publication" \
  -c "select pubname, schemaname, tablename from pg_publication_tables" \
  -c "select slot_name, plugin, active from pg_replication_slots"
```

Kết quả mong đợi: `cdc_order_publication` chứa đúng `public.order_entity` và `public.debezium_heartbeat`
(xem Bước 3), slot `cdc_order_slot` (plugin `pgoutput`)
ở trạng thái `active = t`.

**Tạo publication thủ công** (khi user của Debezium không được phép tạo, ví dụ môi trường production do DBA quản lý):

```sql
-- chạy bằng user sở hữu bảng (hoặc superuser), trong database order_db
CREATE PUBLICATION cdc_order_publication FOR TABLE public.order_entity, public.debezium_heartbeat;
```

rồi đổi `cdc.debezium.publication.autocreate.mode` thành `disabled`.
Lưu ý: `CREATE PUBLICATION ... FOR TABLE` yêu cầu quyền `CREATE` trên database **và** là owner của bảng.
Ở môi trường này user `saga` là owner nên tự tạo được.

Khi thêm bảng vào CDC: với `filtered` chỉ cần sửa `table.include.list` rồi restart app;
với `disabled` phải chạy tay `ALTER PUBLICATION cdc_order_publication ADD TABLE <bảng>;`.

### 3. Bảng heartbeat (chống WAL phình to)

**Vấn đề:** slot chỉ tiến lên khi connector xác nhận (flush) một LSN mới, mà connector chỉ nhận được message
của các bảng trong publication. Nếu `order_entity` ít thay đổi trong khi các bảng khác (Axon `domain_event_entry`,
`token_entry`…) ghi liên tục, slot đứng yên và **giữ toàn bộ WAL** → đầy disk
(xem [production-risks.md mục 1](production-risks.md#1-wal-phình-to-đầy-disk)).

**Giải pháp:** cứ mỗi `heartbeat.interval.ms`, Debezium chạy `heartbeat.action.query` để ghi vào bảng
`debezium_heartbeat`. Bảng này nằm trong publication nên thay đổi đi qua slot, connector nhận được, commit offset
và xác nhận LSN mới nhất cho Postgres, nhờ đó WAL cũ được giải phóng.

Tạo bảng **trước khi** start app. Debezium không tự tạo bảng, và engine sẽ lỗi khi đưa một bảng chưa tồn tại
vào publication:

```sh
docker exec -i saga-axon-kafka-postgres-1 psql -U saga -d order_db < sql/debezium_heartbeat.sql
```

Cấu hình trong `application.yml`:

```yaml
table.include.list: public.order_entity,public.debezium_heartbeat
heartbeat.interval.ms: 10000
heartbeat.action.query: >-
  INSERT INTO public.debezium_heartbeat (id, ts) VALUES (1, now())
  ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts
```

Bảng chỉ có 1 dòng (`id = 1`), được upsert liên tục nên không phình to.

**Không publish lên Kafka:** heartbeat sinh ra 2 loại message, flow `debeziumToKafkaFlow` lọc bỏ cả hai
(chỉ forward destination `cdc.order.public.order_entity`, phần còn lại đi vào `nullChannel`):

| Destination | Nguồn gốc |
|---|---|
| `__debezium-heartbeat.cdc.order` | Heartbeat record của Debezium (mang offset hiện tại) |
| `cdc.order.public.debezium_heartbeat` | Change event của bảng heartbeat |

Hai topic này không được tạo trên Kafka; nếu không lọc, `send().join()` sẽ treo vì broker tắt auto-create.
Engine vẫn coi các message bị lọc là đã xử lý, nên offset vẫn được commit.

**Kết quả đo** (ghi ~61 MB WAL vào một bảng không được capture trong `order_db`):

| | Trước heartbeat | Sau heartbeat |
|---|---|---|
| Lag ngay sau khi ghi | 61 MB | 61 MB |
| Lag sau 30s | 61 MB (đứng yên) | ~400 kB |
| Lag sau 60s | 61 MB (đứng yên) | ~400 bytes |

Thời gian hồi phục ≈ `heartbeat.interval.ms` (10s) + `offset.flush.interval.ms` (5s) + thời gian Postgres decode
lượng WAL tồn đọng.

Kiểm tra heartbeat đang chạy (`ts` phải cập nhật mỗi ~10s):

```sh
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db -c "select ts, now() from debezium_heartbeat"
```

> Heartbeat chỉ xử lý trường hợp **connector đang chạy**. Nếu app dừng hoặc slot mồ côi, WAL vẫn bị giữ;
> cần giám sát lag (xem phần Vận hành) và cân nhắc `max_slot_wal_keep_size`.

### 4. Build & chạy app

```sh
./mvnw package -DskipTests
docker compose up -d --build
docker logs -f cdc-app
```

Khi khởi động, app sẽ:
1. Tạo topic `cdc.order.public.order_entity` (bean `NewTopic`) nếu chưa có.
2. Engine tạo topic offset `cdc-app.debezium-offsets` (compact), replication slot `cdc_order_slot`
   và publication `cdc_order_publication` (xem Bước 2).
3. Lần đầu: snapshot toàn bộ bảng (`__op=r`), sau đó stream WAL. Các lần sau: đọc offset → bỏ qua snapshot.

Biến môi trường (có default để chạy local):

| Env | Default | Trong compose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9192` | `kafka:29092` |
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` | `postgres` |
| `DB_USER` / `DB_PASSWORD` | `saga` / `saga` | — |

Các cấu hình chính trong `cdc.debezium`:

| Key | Giá trị | Ý nghĩa |
|---|---|---|
| `plugin.name` | `pgoutput` | Decoder có sẵn trong Postgres ≥10 |
| `slot.name` / `publication.name` | `cdc_order_slot` / `cdc_order_publication` | Debezium tự tạo |
| `publication.autocreate.mode` | `filtered` | Publication chỉ chứa bảng trong `table.include.list` |
| `topic.prefix` | `cdc.order` | Destination = `cdc.order.public.order_entity` (dùng làm topic Kafka) |
| `snapshot.mode` | `initial` | Snapshot 1 lần khi chưa có offset |
| `offset.mismatch.strategy` | `trust_slot` | Offset tụt sau slot thì stream từ slot thay vì fail (Issue 6) |
| `decimal.handling.mode` | `string` | `numeric` ra `"99.50"` thay vì bytes base64 |
| `heartbeat.interval.ms` / `heartbeat.action.query` | `10000` / upsert `debezium_heartbeat` | Giữ slot luôn tiến lên (Bước 3) |
| `offset.storage` | `KafkaOffsetBackingStore` | Offset lưu ở topic `cdc-app.debezium-offsets`, cần thêm `bootstrap.servers` |
| `key/value.converter.schemas.enable` | `false` | JSON thuần, không có envelope schema |
| `transforms.unwrap` | `ExtractNewRecordState` | Làm phẳng message, thêm `__op`, `__table`, `__source_ts_ms` |
| `...delete.tombstone.handling.mode` | `rewrite` | DELETE ra 1 message với `__deleted=true`, không có tombstone |

**Delivery semantics:** flow gọi `kafkaTemplate.send(...).join()` — engine chỉ commit offset sau khi Kafka ack
→ **at-least-once** (có thể trùng khi app crash giữa chừng, không mất). Consumer nên xử lý idempotent theo `order_id`.

### 5. Kiểm tra end-to-end

```sh
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db \
  -c "insert into order_entity(order_id,amount,customer_id,product_id,quantity,status) values ('cdc-test',42.00,'c9','p9',3,'CREATED')" \
  -c "update order_entity set status='CONFIRMED' where order_id='cdc-test'" \
  -c "delete from order_entity where order_id='cdc-test'"

# Message thô trên topic
docker exec saga-axon-kafka-kafka-1 kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc.order.public.order_entity --from-beginning --property print.key=true --timeout-ms 20000

# Log consumer
docker logs cdc-app | grep '\[CDC\]'
```

Kết quả mong đợi:

```
{"order_id":"cdc-test"}  {"order_id":"cdc-test","amount":"42.00",...,"status":"CREATED","__deleted":"false","__op":"c",...}
[CDC] INSERT order_entity: OrderChangeEvent[orderId=cdc-test, amount=42.00, ..., status=CREATED, op=c, deleted=false, ...]
[CDC] UPDATE order_entity: OrderChangeEvent[orderId=cdc-test, ..., status=CONFIRMED, op=u, deleted=false, ...]
[CDC] DELETE order_entity: OrderChangeEvent[orderId=cdc-test, amount=null, ..., op=d, deleted=true, ...]
```

Mapping `__op`: `r` = snapshot, `c` = insert, `u` = update, `d` = delete.

Kiểm tra resume sau restart: `docker restart cdc-app` → log phải có
`A previous offset indicating a completed snapshot has been found`, `Snapshot ended with SnapshotResult [status=SKIPPED`
và `Retrieved latest position from stored offset`.

## Issues gặp phải & cách giải quyết

### Issue 1 — Postgres đang ở `wal_level = replica`

- **Triệu chứng:** Debezium không thể tạo logical replication slot.
- **Nguyên nhân:** Image `postgres:16-alpine` mặc định `wal_level=replica`.
- **Giải quyết:** `ALTER SYSTEM SET wal_level = logical` + restart container (Bước 1).
  App saga mất kết nối DB vài giây trong lúc restart.

### Issue 2 — Topic không được tạo: `UNKNOWN_TOPIC_OR_PARTITION` *(giai đoạn Kafka Connect)*

- **Triệu chứng:** Connector RUNNING, snapshot `Finished exporting 6 records`, nhưng topic không tồn tại, log lặp:
  ```
  WARN [Producer clientId=connector-producer-order-db-connector-0] The metadata response from the cluster
  reported a recoverable issue ... {cdc.order.public.order_entity=UNKNOWN_TOPIC_OR_PARTITION}
  ```
- **Nguyên nhân:** Broker saga cấu hình `auto.create.topics.enable=false`
  (kiểm tra: `kafka-configs --bootstrap-server kafka:29092 --entity-type brokers --entity-name 1 --describe --all | grep auto.create`).
- **Giải quyết lúc đó:** thêm `topic.creation.default.replication.factor/partitions` vào config connector.
- **Hiện tại (embedded):** app tự tạo topic bằng bean `NewTopic` (Spring `KafkaAdmin`);
  topic offset do `KafkaOffsetBackingStore` tự tạo qua `TopicAdmin`.
  Nếu thêm bảng mới → **phải thêm `NewTopic`** tương ứng, nếu không `send().join()` sẽ treo/timeout.

### Issue 3 — Restart connector nhưng task vẫn treo *(giai đoạn Kafka Connect)*

- **Triệu chứng:** Sau `PUT` config + `POST .../restart?includeTasks=true`, status vẫn RUNNING nhưng không có dữ liệu:
  ```
  WARN Unable to register metrics as an old set with the same name:
  'debezium.postgres:type=connector-metrics,context=snapshot,server=cdc.order' exists, retrying in PT5S
  ```
- **Nguyên nhân:** Task cũ kẹt ở producer (retry gửi vào topic không tồn tại) nên không dừng hẳn.
- **Giải quyết:** `docker restart cdc-connect`. Các thay đổi của 2 dòng test phát sinh trong lúc kẹt không lên Kafka
  (chỉ là dữ liệu test).

### Issue 4 — App chạy trên host không kết nối được Kafka

- **Triệu chứng:**
  ```
  Discovered group coordinator localhost:9092 ...
  WARN Connection to node 2147483646 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
  ```
- **Nguyên nhân:** Kafka saga advertise `PLAINTEXT_HOST://localhost:9092` nhưng map port ra host là `9192->9092`.
  Bootstrap qua `localhost:9192` được, nhưng broker trả về `localhost:9092` — trên host không có gì lắng nghe.
- **Giải quyết:**
  - Trong Docker: chạy cùng network, kết nối `kafka:29092` (service `cdc-app`).
  - Từ IDE (**đã áp dụng** 2026-09-29): sửa compose saga thành
    `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9192` rồi `docker compose up -d kafka`.
    Service trong Docker vẫn dùng `kafka:29092` nên không ảnh hưởng.
- **Ảnh hưởng tới test:** `CdcApplicationTests.contextLoads` khởi động engine thật trỏ vào `localhost` nên chạy
  ~45s (engine không tới được Kafka), nhưng vẫn pass.

### Issue 5 — Sự kiện DELETE chỉ có khoá chính

- **Triệu chứng:** Message DELETE có các cột khác `null`, `quantity = 0`, chỉ còn `order_id`.
- **Nguyên nhân:** `REPLICA IDENTITY DEFAULT` → WAL chỉ ghi giá trị cũ của cột PK.
- **Giải quyết (chưa áp dụng, tuỳ nhu cầu):** `ALTER TABLE order_entity REPLICA IDENTITY FULL;` (WAL lớn hơn).

### Issue 6 — Engine chết khi start: offset tụt sau replication slot

- **Triệu chứng:** App start bình thường nhưng heartbeat không chạy, update `order_entity` không ra event.
  Slot `active = f`, không có dòng nào trong `pg_stat_replication`, thread dump không có thread engine. Log (lẫn giữa
  log config Kafka):
  ```
  ERROR AsyncEmbeddedEngine : 1 task(s) out of 1 failed to start.
  DebeziumException: The connector is trying to read change stream starting at PostgresOffsetContext [...
    lsn=LSN{0/10EA3B30}, lastCommitLsn=LSN{0/10EA3AF8}, timestamp=2026-09-28T09:31:59Z ...],
    but this is no longer available on the server.
  ```
- **Nguyên nhân:** offset lưu trong Kafka (`lsn_commit = 0/10EA3AF8`) **nhỏ hơn** `confirmed_flush_lsn` của slot
  (`0/10EA3BB8`, khớp lần heartbeat cuối 09:32:20). Postgres không trả lại WAL trước `confirmed_flush_lsn`, và với
  strategy mặc định `no_validation` engine dừng hẳn. Chưa xác định được vì sao bản ghi offset cuối không lên
  Kafka (nghi app/Kafka dừng giữa lúc flush).
- **Đã thử:** `trust_greater_lsn` — vẫn `pg_replication_slot_advance` về LSN của offset, Postgres từ chối
  `cannot advance replication slot to 0/10EA3AF8, minimum is 0/10EA3BB8`, engine retry mỗi 10s.
- **Giải quyết:** `offset.mismatch.strategy: trust_slot` → log `Received COMMIT LSN 'LSN{0/10EA3CB0}' larger than
  than last stored commit LSN` → `Processing messages`, heartbeat và CDC chạy lại.
- **Mất dữ liệu?** Khoảng WAL giữa offset và slot (192 byte) bị bỏ qua. Slot chỉ tiến khi Debezium flush, nên
  khoảng này đã được engine xử lý trước đó — nhiều khả năng chỉ là heartbeat, không kiểm chứng được.
- Quy trình đối chiếu chi tiết: xem [Đối chiếu offset với replication slot](#đối-chiếu-offset-với-replication-slot).

### Chuyển từ Kafka Connect sang Spring Integration Debezium

Các bước đã làm để gỡ setup cũ:

```sh
curl -s -X DELETE localhost:8083/connectors/order-db-connector
docker compose rm -sf connect
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db \
  -c "select pg_drop_replication_slot('cdc_order_slot')" \
  -c "drop publication if exists cdc_order_publication"
for t in cdc_connect_configs cdc_connect_offsets cdc_connect_statuses; do
  docker exec saga-axon-kafka-kafka-1 kafka-topics --bootstrap-server kafka:29092 --delete --topic $t
done
```

- Topic dữ liệu `cdc.order.public.order_entity` được **giữ lại** (cùng tên, cùng format JSON) → consumer không phải đổi.
- Vì engine mới không có offset, lần chạy đầu đã **snapshot lại** → topic có thêm 6 message `__op=r` trùng với snapshot cũ.
- Offset format của Kafka Connect và embedded engine khác topic lưu trữ, nên không tái sử dụng được offset cũ.

### Ghi chú khác

- Log engine có `These configurations '[connector.class, slot.name, ...]' were supplied but are not used yet`:
  Kafka client in ra khi nhận property của Debezium — vô hại.
- Log `TypeRegistry: Type [oid:12332, name:_pg_user_mappings] is already mapped`: vô hại.
- Chỉ chạy **1 instance** `cdc-app`: một replication slot chỉ cho 1 kết nối active; instance thứ 2 sẽ lỗi
  `replication slot "cdc_order_slot" is active`.

## Vận hành

```sh
# Replication slot: active + độ trễ WAL
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db -c \
  "select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag from pg_replication_slots"

# Offset engine đã lưu
docker exec saga-axon-kafka-kafka-1 kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc-app.debezium-offsets --from-beginning --property print.key=true --timeout-ms 10000
```

### Đối chiếu offset với replication slot

Vị trí đọc WAL được lưu ở **2 nơi** độc lập:

| Nơi lưu | Giá trị | Định dạng |
|---|---|---|
| Kafka topic `cdc-app.debezium-offsets`, key `["order-db-cdc",{"server":"cdc.order"}]` | `lsn_commit` (commit LSN của transaction cuối đã xử lý), `lsn` / `lsn_proc` (LSN event cuối), `txId`, `ts_usec` | Số thập phân |
| Postgres `pg_replication_slots` | `confirmed_flush_lsn` (Postgres không gửi lại WAL trước mốc này), `restart_lsn` (WAL cũ nhất còn giữ) | `X/Y` hex |

**Vòng đời khi đang chạy:** xử lý event → mỗi `offset.flush.interval.ms` (5s) ghi offset lên Kafka → commit →
Debezium flush `lsn_commit` về slot → `confirmed_flush_lsn` tiến lên → Postgres dọn WAL cũ.
Vì slot chỉ được flush **sau** khi offset đã ghi, trạng thái bình thường là
**`confirmed_flush_lsn` = `lsn_commit`** (đã kiểm chứng) hoặc slot tụt sau offset một chút.

**Khi engine start, chọn vị trí đọc WAL tiếp theo:**

1. Đọc bản ghi offset **mới nhất** theo key từ topic offset. Không có offset → snapshot theo `snapshot.mode`.
2. Đọc trạng thái slot, so sánh với offset theo `offset.mismatch.strategy` (log `Using offset mismatch strategy ...`).
   Offset không còn đọc được từ slot → fail `... but this is no longer available on the server`.
3. Nếu strategy yêu cầu thì seek slot: `SELECT pg_replication_slot_advance('cdc_order_slot', '<lsn_commit>')`
   (log `Seeking to LSN{...} on the replication slot`). Slot chỉ tiến được, **không lùi được**.
4. `START_REPLICATION` trên slot: Postgres stream từ `confirmed_flush_lsn`.
5. `WalPositionLocator` bỏ qua các message đã xử lý (≤ offset đã lưu), tới khi gặp
   `Received COMMIT LSN '...' larger than than last stored commit LSN '...'` → `Processing messages`.

**Lệnh đối chiếu:**

```sh
# 1. Slot: LSN hex + dạng số để so với Kafka
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db -c \
  "select slot_name, active, confirmed_flush_lsn, confirmed_flush_lsn - '0/0'::pg_lsn as confirmed_dec, restart_lsn
   from pg_replication_slots where slot_name = 'cdc_order_slot'"

# 2. Offset mới nhất trong Kafka (dòng cuối)
docker exec saga-axon-kafka-kafka-1 kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic cdc-app.debezium-offsets --from-beginning --property print.key=true --timeout-ms 8000 | tail -1
# ["order-db-cdc",{"server":"cdc.order"}]  {"lsn_proc":283853840,"lsn_commit":283853728,"lsn":283853840,"txId":313797,...}

# 3. Đổi số thập phân của Kafka sang hex để đọc log / so với slot
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db -tAc "select '0/0'::pg_lsn + 283853728"
# 0/10EB43A0   (hex → số: select '0/10EB43A0'::pg_lsn - '0/0'::pg_lsn)
```

**Đọc kết quả** (so `lsn_commit` với `confirmed_dec`):

| Tình huống | Ý nghĩa | Hành động |
|---|---|---|
| Bằng nhau | Bình thường | — |
| Offset > slot | Slot flush chậm hơn offset (vd. app dừng ngay sau khi ghi offset) | Bình thường. Postgres còn giữ WAL, engine lọc phần đã xử lý |
| Offset < slot | Offset mất bản ghi cuối, topic offset bị restore bản cũ, hoặc slot bị advance từ bên ngoài | Cần `trust_slot` (Issue 6). Kiểm tra khoảng WAL bị bỏ qua |
| Slot `restart_lsn` mới hơn offset rất nhiều, slot mới được tạo | Slot bị drop/tạo lại → mất thay đổi ở giữa | Snapshot lại (mục dưới). Dùng `trust_offset` để phát hiện sớm |
| Không có slot | Slot bị drop | Engine tự tạo slot mới tại vị trí hiện tại → mất thay đổi ở giữa, không báo lỗi (đã kiểm chứng, xem [test](test-scenarios.md#replication-slot-bị-xoá)). Snapshot lại |

### Restart khi offset đi trước slot

Ví dụ: offset `lsn_commit = 1000`, slot `confirmed_flush_lsn = 999`.

**Vì sao xảy ra:** offset = "đã giao xong", slot luôn đi sau offset một nhịp:
1. Handler chạy `kafkaTemplate.send(...).join()`, block tới khi Kafka ack.
2. Handler return → engine đánh dấu record đã xử lý → ghi offset lên Kafka (mỗi 5s).
3. Sau đó thread `lsn-flush` mới flush `lsn_commit` về slot.

App dừng giữa bước 2 và 3 → offset 1000, slot 999.

**Khi restart:**
- Postgres gửi lại từ 999, gồm cả txn 1000.
- Debezium đọc, decode rồi **bỏ** txn 1000 (LSN ≤ offset): không gửi Kafka, không ghi bản ghi offset, không flush slot.
  Slot **vẫn 999**. Keepalive cũng báo 999, vì flush mode `connector` tắt keepalive flush của driver.
- Filter chỉ chạy lúc khởi động. Gặp txn đầu tiên chưa xử lý → `switching off the filtering` → xử lý bình thường.
  Crash giữa txn thì resume theo `lsn` (event cuối), chạy tiếp từ event kế trong txn đó.
- An toàn vì logical decoding gửi txn theo **thứ tự commit** → commit LSN tăng dần → LSN ≤ offset chắc chắn đã xử lý.

**Slot chỉ tiến khi có txn mới được xử lý** (heartbeat ≤10s hoặc thay đổi bảng order): record qua flow → offset ghi
ở lần flush kế (5s) → flush về slot → `confirmed_flush_lsn` = `lsn_commit` offset mới nhất. Tức là khoảng 10–15s
sau restart là bằng nhau.
- Slot **nhảy thẳng qua** 1000 lên LSN của txn mới, không dừng ở 1000.
- "Bằng nhau" là trạng thái nghỉ. Giữa lúc ghi offset và flush slot, offset luôn đi trước một chút — bình thường.
- Không có heartbeat và bảng order không đổi → slot đứng ở 999 mãi (chỉ giữ thêm ít WAL). Heartbeat chỉ chạy khi app chạy.

**Kiểm chứng (2026-09-29, `trust_slot`):**

| | Trước restart | Sau restart ~90s |
|---|---|---|
| Slot `confirmed_flush_lsn` | `0/10EB6678` | `0/10EB9708` (= `lsn_commit` offset mới nhất) |
| Offset `lsn_commit` | `0/10EB7428` = COMMIT txn `313809` (UPDATE `order_entity`, đã publish) | `0/10EB9708` |
| Bản ghi topic offset | 1287 | 1295 (toàn bộ là heartbeat mới) |
| End offset topic order | 32 | 32 → **không trùng** |

Log:
```
Looking for WAL restart position for last commit LSN 'LSN{0/10EB7428}' ...
found previous flushed LSN 'LSN{0/10EB6678}'
Received COMMIT LSN 'LSN{0/10EB7D88}' larger than than last stored commit LSN 'LSN{0/10EB7428}'
Will restart from LSN 'LSN{0/10EB7D00}' that is start of the first unprocessed transaction
Message with LSN 'LSN{0/10EB7D00}' arrived, switching off the filtering
```

Xem các txn trong khoảng slot → hiện tại (không làm slot tiến, chỉ chạy khi slot **không** active):

```sh
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db -c "
select lsn, lsn - '0/0'::pg_lsn as lsn_dec, xid, chr(get_byte(data,0)) as msg,  -- B/R/I/U/D/C
       case when get_byte(data,0) = ascii('R') then encode(substring(data from 6 for 40),'escape') end as rel
from pg_logical_slot_peek_binary_changes('cdc_order_slot', NULL, NULL,
       'proto_version','1','publication_names','cdc_order_publication')"
```

### Khi nào event bị bỏ qua vĩnh viễn

Việc lọc chỉ an toàn khi offset đúng. Event mất hẳn khi offset ghi nhận "đã xử lý" một event chưa thực sự tới đích:

| Rủi ro | Chi tiết | Phòng tránh |
|---|---|---|
| Flow bỏ event nhưng vẫn commit offset | Filter trong `DebeziumConfig` đẩy mọi destination khác topic order vào `nullChannel`. Thêm bảng vào `table.include.list` mà quên sửa filter → event bảng đó mất im lặng | Thêm bảng thì sửa filter + `NewTopic`. Cân nhắc chỉ bỏ destination heartbeat, fail với destination lạ |
| Exception bị nuốt | Hiện tại adapter không có `errorChannel` → exception từ `send().join()` đi ngược lên engine, record không bị đánh dấu xử lý (đã decompile Spring Integration 7.1.1 `MessageProducerSupport.sendMessage`). Thêm `.errorChannel(...)`, try/catch quanh `send()`, bỏ `.join()` hoặc chèn executor channel → mất event khi Kafka lỗi | Giữ flow đồng bộ, không nuốt exception |
| DB đổi "vũ trụ LSN" (restore backup, PITR, cluster mới) | LSN mới thấp hơn offset cũ → mọi thay đổi tới khi vượt mốc offset bị lọc, không báo lỗi. Không strategy nào cứu được (`trust_offset` còn advance slot lên theo offset). *Suy ra từ cơ chế lọc, chưa test* | Restore/đổi DB → reset offset + snapshot lại (mục dưới). Promote physical standby thì LSN nối tiếp, OK (cần failover slot PG17) |
| Kafka mất message đã ack | Topic data và offset đều replication factor 1 → mất disk broker là mất cả hai | Production: RF ≥ 3 |

Ngoài ra engine có thể chết trong khi app vẫn chạy (Issue 6) → giám sát `pg_replication_slots.active`.

### Offset mismatch strategy

Property `offset.mismatch.strategy` (Debezium Postgres 3.5.2). Mô tả lấy từ log của connector, kèm kết quả đã thử:

| Giá trị | Hành vi | Đã thử |
|---|---|---|
| `no_validation` (mặc định) | Không so sánh với slot, stream từ offset đã lưu. Không phát hiện slot bị tạo lại | Offset < slot → **fail**, engine dừng |
| `trust_offset` | Offset đi trước → advance slot tới offset. Slot đi trước → **fail** (để phát hiện slot bị tạo lại) | Chưa thử |
| `trust_slot` | Slot là chuẩn. Slot đi trước → nâng offset lên theo slot | Offset < slot → **chạy được** (đang dùng) |
| `trust_greater_lsn` | Mô tả: đồng bộ về `max(offset_lsn, slot_lsn)`, tự hồi phục theo cả 2 chiều | Offset < slot → vẫn seek slot lùi về offset, Postgres từ chối, **retry mãi** |

**Lựa chọn:**
- `trust_slot` (đang dùng): ưu tiên pipeline tự chạy lại. Đánh đổi: nếu slot bị drop/tạo lại thì tiến trình
  **âm thầm** bỏ qua khoảng dữ liệu bị mất. Nên giám sát slot (lệnh ở trên) và alert khi slot bị tạo lại.
  Kịch bản test: [Replication slot bị xoá](test-scenarios.md#replication-slot-bị-xoá).
- `trust_offset`: ưu tiên phát hiện mất dữ liệu. Offset < slot thì fail, cần người xử lý (snapshot lại hoặc tạm chuyển
  `trust_slot`).
- `trust_slot` khi offset > slot: không phát lại event đã xử lý (xem [Restart khi offset đi trước slot](#restart-khi-offset-đi-trước-slot)).
- Consumer vẫn cần idempotent theo `order_id` (at-least-once khi app crash giữa lúc gửi Kafka và ghi offset).

### Snapshot lại từ đầu

Dừng app, xoá topic offset (hoặc đổi `cdc.debezium.name` / `offset.storage.topic`), rồi start lại:

```sh
docker compose stop cdc-app
docker exec saga-axon-kafka-kafka-1 kafka-topics --bootstrap-server kafka:29092 --delete --topic cdc-app.debezium-offsets
docker compose start cdc-app
```

### Gỡ bỏ hoàn toàn

> ⚠️ Replication slot giữ WAL cho tới khi được consume. Nếu dừng app lâu mà **không** xoá slot,
> WAL sẽ tích tụ và làm đầy disk Postgres.

```sh
docker compose down
docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db \
  -c "select pg_drop_replication_slot('cdc_order_slot')" \
  -c "drop publication if exists cdc_order_publication" \
  -c "drop table if exists debezium_heartbeat"
docker exec saga-axon-kafka-kafka-1 kafka-topics --bootstrap-server kafka:29092 --delete --topic cdc-app.debezium-offsets
```
