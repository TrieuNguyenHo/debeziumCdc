package org.claude.cdc.order;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCdcListener {

    private final JsonMapper jsonMapper;

    @KafkaListener(topics = "${cdc.topics.order}")
    public void onOrderChange(String payload) {
        OrderChangeEvent event = jsonMapper.readValue(payload, OrderChangeEvent.class);
        String operation = switch (event.op()) {
            case "r" -> "SNAPSHOT";
            case "c" -> "INSERT";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default -> event.op();
        };
        log.info("[CDC] {} order_entity: {}", operation, event);
    }
}
