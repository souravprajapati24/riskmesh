package com.riskmesh.ingestion.security;


public interface MerchantSecretResolver {
    byte[] resolve(String merchantId);
}
