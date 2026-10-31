package com.riskmesh.ingestion.gateway.exception;


public class GatewayRejectedException extends RuntimeException {

    public GatewayRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
