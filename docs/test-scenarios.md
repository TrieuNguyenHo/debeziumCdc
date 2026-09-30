# Test Scenarios

Kịch bản test thực tế cho pipeline CDC. Setup, lệnh vận hành và giải thích cơ chế: xem [debezium-setup.md](debezium-setup.md).

## Replication slot bị xoá

*Đã kiểm chứng 2026-09-30 (heartbeat tắt khi test): kết quả đúng như mô tả ở bước 6 và 8.*

Mục tiêu: kiểm chứng rằng với `trust_slot` + `snapshot.mode: initial`, slot bị drop thì engine tự tạo slot mới ở vị
trí WAL hiện tại, bỏ qua khoảng thay đổi ở giữa mà không báo lỗi.

Lưu ý: Postgres không cho drop slot đang active (`replication slot "cdc_order_slot" is active for PID ...`), nên
kịch bản chính là drop khi app đã dừng.

**0. Chuẩn bị & ghi mốc**

```sh
PG="docker exec saga-axon-kafka-postgres-1 psql -U saga -d order_db"
KC="docker exec saga-axon-kafka-kafka-1 kafka-console-consumer --bootstrap-server kafka:29092"

# Slot (dùng lại nhiều lần)
$PG -c "select slot_name, active, active_pid, confirmed_flush_lsn, confirmed_flush_lsn - '0/0'::pg_lsn as confirmed_dec, restart_lsn from pg_replication_slots"

# Offset mới nhất: ghi lại lsn_commit
$KC --topic cdc-app.debezium-offsets --from-beginning --property print.key=true --timeout-ms 8000 | tail -1
```

**1. Mốc đối chứng** (app đang chạy):

```sh
$PG -c "insert into order_entity(order_id,amount,customer_id,product_id,quantity,status) values ('slot-before',1,'c','p',1,'CREATED')"
```

**2. Dừng app**, slot phải `active = f`:

```sh
docker compose stop cdc-app
$PG -c "select slot_name, active from pg_replication_slots"
```

**3. Thay đổi khi slot còn** (nằm trong khoảng sẽ mất):

```sh
$PG -c "insert into order_entity(order_id,amount,customer_id,product_id,quantity,status) values ('slot-gap-1',1,'c','p',1,'CREATED')" \
    -c "update order_entity set status='CONFIRMED' where order_id='slot-gap-1'"
```

**4. Drop slot:**

```sh
$PG -c "select pg_drop_replication_slot('cdc_order_slot')"
$PG -c "select count(*) from pg_replication_slots"   # 0
```

**5. Thay đổi khi không còn slot:**

```sh
$PG -c "insert into order_entity(order_id,amount,customer_id,product_id,quantity,status) values ('slot-gap-2',1,'c','p',1,'CREATED')"
```

**6. Start app, đọc log:**

```sh
docker compose start cdc-app
docker logs -f cdc-app 2>&1 | grep -iE "slot|mismatch|snapshot|LSN|no longer available|ERROR"
```

Cần xem: log tạo slot mới, `Using offset mismatch strategy ...` và offset có được nâng theo slot không, snapshot có
`SKIPPED` không, có `ERROR` / `no longer available` không. Chạy lại lệnh slot ở bước 0: slot mới `active = t`,
`confirmed_dec` lớn hơn nhiều so với `lsn_commit` đã ghi.

**Slot mới bắt đầu từ đâu.** Ví dụ WAL đang ở `1000` khi Debezium tạo slot:

1. Postgres đặt `restart_lsn` ≈ `1000` (ghi một record `running_xacts`).
2. Đọc WAL từ đó tới *consistent point*: mốc mà mọi transaction đang mở lúc tạo slot đã kết thúc.
3. Đặt `confirmed_flush_lsn` = consistent point ngay khi tạo, không cần Debezium flush.

- Không có transaction đang mở → `confirmed_flush_lsn` chỉ hơn `1000` vài chục byte.
- Có transaction đang mở → lệnh tạo slot **chờ** tới khi chúng kết thúc (engine có thể treo lúc start nếu DB có
  transaction chạy lâu), `confirmed_flush_lsn` nằm sau `1000` xa hơn.
- Luôn có `restart_lsn ≤ confirmed_flush_lsn`. Postgres stream từ `confirmed_flush_lsn`, mọi thay đổi trước đó
  (`slot-gap-*`) không bao giờ được gửi. Sau đó slot chỉ tiến khi Debezium xử lý và flush txn mới
  (xem [Restart khi offset đi trước slot](debezium-setup.md#restart-khi-offset-đi-trước-slot)).

*Theo cơ chế tạo logical slot của Postgres, chưa kiểm chứng trên môi trường này.* Kiểm tra ngay sau khi start, trước
khi có txn nào được xử lý:

```sh
$PG -c "select pg_current_wal_lsn(), restart_lsn, confirmed_flush_lsn from pg_replication_slots where slot_name='cdc_order_slot'"
```

**7. Thay đổi sau khi có slot mới:**

```sh
$PG -c "insert into order_entity(order_id,amount,customer_id,product_id,quantity,status) values ('slot-after',1,'c','p',1,'CREATED')"
```

**8. Kiểm tra topic:**

```sh
$KC --topic cdc.order.public.order_entity --from-beginning --property print.key=true --timeout-ms 15000 | grep "slot-"
docker logs cdc-app 2>&1 | grep "\[CDC\]" | grep "slot-"
```

| order_id | Kết quả |
|---|---|
| `slot-before` | Có |
| `slot-gap-1` (insert + update) | **Không** |
| `slot-gap-2` | **Không** |
| `slot-after` | Có |
| Message `__op=r` mới | Không (không snapshot lại) |

→ Xác nhận drop slot với `trust_slot` làm mất dữ liệu im lặng.

**9. (Tuỳ chọn) Khôi phục:** làm theo [Snapshot lại từ đầu](debezium-setup.md#snapshot-lại-từ-đầu). `slot-gap-1` (`CONFIRMED`) và
`slot-gap-2` lên topic với `__op=r`, kèm bản sao mọi dòng khác. Update trung gian của `slot-gap-1` vẫn mất (snapshot
chỉ có trạng thái cuối).

**10. Dọn dẹp:**

```sh
$PG -c "delete from order_entity where order_id like 'slot-%'"
```

**Kịch bản phụ — drop khi app đang chạy** (*chưa chạy*):

```sh
$PG -c "select pg_drop_replication_slot('cdc_order_slot')"
# Dự đoán: ERROR: replication slot "cdc_order_slot" is active for PID ...

$PG -c "select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name='cdc_order_slot'" \
    -c "select pg_drop_replication_slot('cdc_order_slot')"
```

Engine reconnect nhanh có thể làm drop lại báo `is active` → chạy lại. Sau đó theo dõi `docker logs -f cdc-app`: engine
reconnect và tạo slot mới, hay chết hẳn (slot không xuất hiện / `active = f`). Rồi lặp lại bước 7–8.

