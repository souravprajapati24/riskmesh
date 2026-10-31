package com.riskmesh.ingestion.gateway;

import com.riskmesh.ingestion.config.GatewayProperties;
import java.math.BigDecimal;
import java.util.UUID;

import com.riskmesh.ingestion.gateway.exception.GatewayRejectedException;
import com.riskmesh.ingestion.gateway.exception.GatewayUnavailableException;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@Component
public class MockGatewayClient {

    private final RestClient restClient;
    private final String holdPath;
    private final ObjectMapper objectMapper;

    public MockGatewayClient(GatewayProperties properties , ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout((int) properties.getConnectTimeout().toMillis());
        requestFactory.setReadTimeout((int) properties.getReadTimeout().toMillis());
        this.restClient = RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .requestFactory(requestFactory)
                .build();
        this.holdPath = properties.getHoldPath();
    }

    public HoldResult placeHold(UUID transactionId, BigDecimal amount, String currency) {
        if (transactionId == null || amount == null || currency == null) {
            throw new IllegalArgumentException(
                    "Hold request requires transactionId, amount, and currency"
            );
        }

        HoldResponseBody response;

        try {
            String requestJson = objectMapper.writeValueAsString(
                    new HoldRequestBody(
                            transactionId.toString(),
                            amount.toPlainString(),
                            currency
                    )
            );

            response = restClient.post()
                    .uri(holdPath)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(requestJson)
                    .retrieve()
                    .body(HoldResponseBody.class);

        } catch (JsonProcessingException e) {
            throw new GatewayUnavailableException(
                    "Failed to serialize gateway request", e
            );
        } catch (HttpClientErrorException.UnprocessableEntity e) {
            throw new GatewayRejectedException(
                    "Gateway rejected the hold (HTTP 422): "
                            + e.getResponseBodyAsString(),
                    e
            );
        } catch (HttpStatusCodeException e) {
            throw new GatewayUnavailableException(
                    "Gateway returned HTTP " + e.getStatusCode().value(), e
            );
        } catch (ResourceAccessException e) {
            throw new GatewayUnavailableException(
                    "Gateway unreachable: " + e.getMessage(), e
            );
        } catch (RestClientException e) {
            throw new GatewayUnavailableException(
                    "Gateway call failed: " + e.getMessage(), e
            );
        }

        if (response == null
                || response.holdReference() == null
                || response.holdReference().isBlank()) {
            throw new GatewayUnavailableException(
                    "Gateway response did not contain a holdReference", null
            );
        }

        return new HoldResult(response.holdReference());
    }

    public record HoldRequestBody(String transactionId, String amount, String currency) {}

    public record HoldResponseBody(String holdReference) {}
}
