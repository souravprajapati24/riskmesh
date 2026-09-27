package com.riskmesh.ingestion.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.riskmesh.ingestion.domain.enums.PaymentMethod;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.UUID;

public class TransactionRequest {

    @NotBlank
    @Size(max = 128)
    private String externalTxnId;

    @NotNull
    private UUID merchantId;

    @NotNull
    private UUID payerId;

    @NotNull
    private UUID payeeId;

    @NotNull
    @DecimalMin(value = "0.01")
    @JsonDeserialize(using = BigDecimalStringDeserializer.class)
    private BigDecimal amount;

    @NotBlank
    @Pattern(regexp = "[A-Z]{3}")
    private String currency;

    @NotNull
    private PaymentMethod paymentMethod;

    @Size(min = 6, max = 6)
    private String cardBin;

    @Size(min = 4, max = 4)
    private String cardLast4;

    private String deviceFingerprint;
    private String ipAddress;
    private String geoCountry;
    private String geoCity;
    private String merchantCategory;
    private String userAgent;

    public String getExternalTxnId() {
        return externalTxnId;
    }

    public void setExternalTxnId(String externalTxnId) {
        this.externalTxnId = externalTxnId;
    }

    public UUID getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(UUID merchantId) {
        this.merchantId = merchantId;
    }

    public UUID getPayerId() {
        return payerId;
    }

    public void setPayerId(UUID payerId) {
        this.payerId = payerId;
    }

    public UUID getPayeeId() {
        return payeeId;
    }

    public void setPayeeId(UUID payeeId) {
        this.payeeId = payeeId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public PaymentMethod getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(PaymentMethod paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getCardBin() {
        return cardBin;
    }

    public void setCardBin(String cardBin) {
        this.cardBin = cardBin;
    }

    public String getCardLast4() {
        return cardLast4;
    }

    public void setCardLast4(String cardLast4) {
        this.cardLast4 = cardLast4;
    }

    public String getDeviceFingerprint() {
        return deviceFingerprint;
    }

    public void setDeviceFingerprint(String deviceFingerprint) {
        this.deviceFingerprint = deviceFingerprint;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getGeoCountry() {
        return geoCountry;
    }

    public void setGeoCountry(String geoCountry) {
        this.geoCountry = geoCountry;
    }

    public String getGeoCity() {
        return geoCity;
    }

    public void setGeoCity(String geoCity) {
        this.geoCity = geoCity;
    }

    public String getMerchantCategory() {
        return merchantCategory;
    }

    public void setMerchantCategory(String merchantCategory) {
        this.merchantCategory = merchantCategory;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    @AssertTrue(message = "cardBin and cardLast4 are required when paymentMethod is CARD")
    public boolean isCardFieldsPresentWhenCard() {
        if (paymentMethod != PaymentMethod.CARD) {
            return true;
        }
        return cardBin != null && !cardBin.isBlank() && cardLast4 != null && !cardLast4.isBlank();
    }
}
