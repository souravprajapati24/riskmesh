package com.riskmesh.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@Component
@ConfigurationProperties(prefix = "riskmesh.security")
public class RiskMeshSecurityProperties {

    private final Hmac hmac = new Hmac();
    private final RateLimit rateLimit = new RateLimit();

    public Hmac getHmac() {
        return hmac;
    }

    public RateLimit getRateLimit() {
        return rateLimit;
    }

    public static class Hmac {
        private String sharedSecret;

        public String getSharedSecret() {
            return sharedSecret;
        }

        public void setSharedSecret(String sharedSecret) {
            this.sharedSecret = sharedSecret;
        }
    }

    public static class RateLimit {
        private int perMerchantLimit;
        private int globalLimit;
        private int ipViolationThreshold;

        public int getPerMerchantLimit() {
            return perMerchantLimit;
        }

        public void setPerMerchantLimit(int perMerchantLimit) {
            this.perMerchantLimit = perMerchantLimit;
        }

        public int getGlobalLimit() {
            return globalLimit;
        }

        public void setGlobalLimit(int globalLimit) {
            this.globalLimit = globalLimit;
        }

        public int getIpViolationThreshold() {
            return ipViolationThreshold;
        }

        public void setIpViolationThreshold(int ipViolationThreshold) {
            this.ipViolationThreshold = ipViolationThreshold;
        }
    }
}
