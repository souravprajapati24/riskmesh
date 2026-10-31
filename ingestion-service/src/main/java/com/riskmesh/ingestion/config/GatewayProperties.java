package com.riskmesh.ingestion.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(prefix = "riskmesh.gateway")
public class GatewayProperties {

    private String baseUrl;
    private String holdPath;
    private Duration connectTimeout;
    private Duration readTimeout;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getHoldPath() {
        return holdPath;
    }

    public void setHoldPath(String holdPath) {
        this.holdPath = holdPath;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
    }
}
