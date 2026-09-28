package org.claude.cdc.order;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/**
 * A row change of order_db.public.order_entity, as produced by Debezium with the
 * ExtractNewRecordState SMT (row state flattened, metadata added as __-prefixed fields).
 */
public record OrderChangeEvent(
        @JsonProperty("order_id") String orderId,
        BigDecimal amount,
        @JsonProperty("customer_id") String customerId,
        @JsonProperty("product_id") String productId,
        int quantity,
        @JsonProperty("rejection_reason") String rejectionReason,
        String status,
        @JsonProperty("__op") String op,
        @JsonProperty("__deleted") boolean deleted,
        @JsonProperty("__source_ts_ms") long sourceTsMs
) {
}
