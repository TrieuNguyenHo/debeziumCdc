package org.claude.cdc.debezium;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;
import java.util.Properties;

/**
 * @param debezium raw Debezium engine properties (connector, offset store, transforms...)
 */
@ConfigurationProperties("cdc")
public record CdcProperties(Map<String, String> debezium) {

    public Properties debeziumProperties() {
        Properties properties = new Properties();
        properties.putAll(debezium);
        return properties;
    }
}
