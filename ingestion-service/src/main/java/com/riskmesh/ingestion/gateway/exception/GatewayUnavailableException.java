package com.riskmesh.ingestion.gateway.exception;


public class GatewayUnavailableException extends RuntimeException {

    public GatewayUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
