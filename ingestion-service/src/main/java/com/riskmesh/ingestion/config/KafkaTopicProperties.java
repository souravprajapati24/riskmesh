package com.riskmesh.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(prefix = "riskmesh.kafka.topics")
public class KafkaTopicProperties {

    private short replicationFactor;
    private int minInsyncReplicas;

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public int getMinInsyncReplicas() {
        return minInsyncReplicas;
    }

    public void setMinInsyncReplicas(int minInsyncReplicas) {
        this.minInsyncReplicas = minInsyncReplicas;
    }
}
