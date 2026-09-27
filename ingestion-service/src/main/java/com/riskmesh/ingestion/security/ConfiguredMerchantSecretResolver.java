package com.riskmesh.ingestion.security;

import com.riskmesh.ingestion.config.RiskMeshSecurityProperties;
import java.nio.charset.StandardCharsets;
import org.springframework.stereotype.Component;


@Component
public class ConfiguredMerchantSecretResolver implements MerchantSecretResolver {

    private final byte[] sharedSecret;

    public ConfiguredMerchantSecretResolver(RiskMeshSecurityProperties properties) {
        this.sharedSecret = properties.getHmac().getSharedSecret().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] resolve(String merchantId) {
        return sharedSecret;
    }
}
