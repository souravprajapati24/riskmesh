package com.riskmesh.ingestion.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "riskmesh.outbox")
public class OutboxProperties {

    private Duration escalationAfter;

    public Duration getEscalationAfter() {
        return escalationAfter;
    }

    public void setEscalationAfter(Duration escalationAfter) {
        this.escalationAfter = escalationAfter;
    }
}
