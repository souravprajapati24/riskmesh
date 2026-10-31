package com.riskmesh.common.events;


public final class KafkaTopics {


    public static final String TRANSACTION_RECEIVED = "transaction.received";
    public static final String TRANSACTION_SCORED = "transaction.scored";
    public static final String FUND_HOLD_REQUESTED = "fund.hold.requested";
    public static final String FUND_RELEASED = "fund.released";
    public static final String FUND_CAPTURED = "fund.captured";
    public static final String FUND_HOLD_FAILED = "fund.hold.failed";
    public static final String CASE_REVIEWED = "case.reviewed";
    public static final String NOTIFICATION_SENT = "notification.sent";
    public static final String TRANSACTION_RECEIVED_DLQ = "transaction.received.dlq";
    public static final String TRANSACTION_SCORED_DLQ = "transaction.scored.dlq";
    public static final String FUND_HOLD_CONFIRMED = "fund.hold.confirmed";
    public static final String FUND_HOLD_VOIDED = "fund.hold.voided";
    public static final String SAGA_COMPENSATED = "saga.compensated";
    public static final String STEP_UP_COMPLETED = "step_up.completed";

    private KafkaTopics() {
    }
}
