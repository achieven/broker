// The Kafka client logs every retriable error at ERROR level even when the retry succeeds (e.g. CONCURRENT_TRANSACTIONS
// right after an abort), and the Consumer logs each message. That noise buries real failures, so silence it here.
// Re-enable with VERBOSE_TESTS=1 or KAFKAJS_LOG_LEVEL=debug (honoured by BrokerService).
if (!process.env.VERBOSE_TESTS) {
    process.env.KAFKAJS_LOG_LEVEL ??= 'nothing';
    console.log = () => {};
}
