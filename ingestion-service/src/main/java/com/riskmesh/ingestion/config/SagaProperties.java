package com.riskmesh.ingestion.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "riskmesh.saga")
public class SagaProperties {

    private Duration stalledThreshold;

    public Duration getStalledThreshold() {
        return stalledThreshold;
    }

    public void setStalledThreshold(Duration stalledThreshold) {
        this.stalledThreshold = stalledThreshold;
    }
}
