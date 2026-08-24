package com.riskmesh.common.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

@Component
public class TransactionIdMdcInterceptor implements RecordInterceptor<String, String> {

    private static final String TRANSACTION_ID_MDC_KEY = "transactionId";

    @Override
    public ConsumerRecord<String, String> intercept(
            ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
        MDC.put(TRANSACTION_ID_MDC_KEY, record.key());
        return record;
    }
}
