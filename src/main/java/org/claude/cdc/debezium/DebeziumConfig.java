package org.claude.cdc.debezium;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.debezium.dsl.Debezium;
import org.springframework.integration.debezium.support.DebeziumHeaders;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Runs the Debezium Postgres connector embedded in this app (no Kafka Connect)
 * and forwards every change event to the Kafka topic Debezium names for it.
 */
@Configuration
@EnableConfigurationProperties(CdcProperties.class)
public class DebeziumConfig {

    @Bean
    IntegrationFlow debeziumToKafkaFlow(CdcProperties cdcProperties, KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        return IntegrationFlow.from(Debezium.inboundChannelAdapter(cdcProperties.debeziumProperties()))
                .handle(message -> {
                    String topic = message.getHeaders().get(DebeziumHeaders.DESTINATION, String.class);
                    byte[] key = message.getHeaders().get(DebeziumHeaders.KEY, byte[].class);
                    // Block until Kafka acks, so the engine only commits offsets of delivered events.
                    kafkaTemplate.send(topic, key, (byte[]) message.getPayload()).join();
                })
                .get();
    }

    // The broker has auto.create.topics.enable=false.
    @Bean
    NewTopic orderTopic(@Value("${cdc.topics.order}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
}
