import { KafkaJS } from '@confluentinc/kafka-javascript';
import { BrokerService } from '../broker.service';
import { Mutex } from './retry';
import { MessageMeta } from './types';

export interface DlqEntry {
    meta: MessageMeta;
    message: KafkaJS.KafkaMessage;
    stage: string;
    /** `x-error-class`: permanent | exhausted | tombstone | duplicate */
    errorClass: string;
    /** Legacy `dlq-reason` header, kept so existing DLQ readers keep working. */
    reason: string;
    error?: unknown;
}

export interface DlqWriter {
    /** One attempt. Throws if the record could not be written (and, for the transactional writer, the offset not committed). */
    write(entry: DlqEntry): Promise<void>;
    disconnect(): Promise<void>;
}

export const MAX_ERROR_HEADER = 1000;

/** Original key and value are preserved; original headers are kept and the failure context is added next to them. */
export function dlqHeaders(entry: DlqEntry): KafkaJS.IHeaders {
    const error = String((entry.error as Error | undefined)?.message ?? entry.error ?? '').slice(0, MAX_ERROR_HEADER);
    const { meta } = entry;
    return {
        ...(entry.message.headers as KafkaJS.IHeaders),
        'x-stage': entry.stage,
        'x-error-class': entry.errorClass,
        'x-error': error,
        'x-source-topic': meta.topic,
        'x-source-partition': String(meta.partition),
        'x-source-offset': meta.offset,
        // legacy names
        'dlq-reason': entry.reason,
        'dlq-error': error,
        'dlq-source-topic': meta.topic,
        'dlq-source-partition': String(meta.partition),
        'dlq-source-offset': meta.offset,
    };
}

const nextOffset = (offset: string) => (BigInt(offset) + 1n).toString();

/**
 * Strict mode: the DLQ record and the source offset are committed in ONE Kafka transaction, so after a crash there is
 * either both (message dead-lettered and consumed) or neither (message redelivered). Transactional + idempotent producer,
 * acks=-1, maxInFlightRequests=1; transactions are serialised because a producer runs only one at a time.
 *
 * The offsets are sent with the consumer's own group metadata (librdkafka), so the broker fences a consumer that was
 * kicked out of the group. Duplicate processing is still possible around rebalances; idempotency covers it.
 */
export class TransactionalDlqWriter implements DlqWriter {
    private readonly mutex = new Mutex();
    private producer?: KafkaJS.Producer;
    private consumer?: KafkaJS.Consumer;
    private connecting?: Promise<void>;

    constructor(
        private readonly broker: BrokerService,
        private readonly dlqTopic: string,
        private readonly transactionalId: string,
    ) {}

    /** The consumer whose offsets are committed inside the DLQ transaction (librdkafka needs the consumer, not a group id). */
    attachConsumer(consumer: KafkaJS.Consumer): void {
        this.consumer = consumer;
    }

    connect(): Promise<void> {
        this.connecting ??= (async () => {
            this.producer = this.broker.kafka.producer({
                kafkaJS: { transactionalId: this.transactionalId, idempotent: true, acks: -1, maxInFlightRequests: 1 },
            });
            await this.producer.connect();
        })().catch((err) => {
            this.connecting = undefined; // a failed connect must not be cached; the next write tries again
            throw err;
        });
        return this.connecting;
    }

    write(entry: DlqEntry): Promise<void> {
        return this.mutex.run(async () => {
            await this.connect();
            if (!this.consumer) throw new Error('TransactionalDlqWriter has no consumer attached');
            const tx = await this.producer!.transaction();
            try {
                await tx.send({
                    topic: this.dlqTopic,
                    messages: [{ key: entry.message.key, value: entry.message.value, headers: dlqHeaders(entry) }],
                });
                await tx.sendOffsets({
                    consumer: this.consumer,
                    topics: [{
                        topic: entry.meta.topic,
                        partitions: [{ partition: entry.meta.partition, offset: nextOffset(entry.meta.offset) }],
                    }],
                });
                await tx.commit();
            } catch (err) {
                try { await tx.abort(); } catch { /* already aborted / transaction ended */ }
                throw err;
            }
        });
    }

    async disconnect(): Promise<void> {
        if (!this.connecting) return;
        await this.connecting.catch(() => {});
        await this.producer?.disconnect();
    }
}

/**
 * Legacy mode: a plain idempotent producer, no offset coupling (the offset is auto-committed by the client).
 * Connected lazily, so consumers that never dead-letter never open a producer connection.
 */
export class PlainDlqWriter implements DlqWriter {
    private producer?: KafkaJS.Producer;
    private connecting?: Promise<void>;

    constructor(private readonly broker: BrokerService, private readonly dlqTopic: string) {}

    connect(): Promise<void> {
        this.connecting ??= (async () => {
            this.producer = this.broker.kafka.producer({ kafkaJS: { idempotent: true, acks: -1, maxInFlightRequests: 1 } });
            await this.producer.connect();
        })().catch((err) => {
            this.connecting = undefined; // a failed connect must not be cached; the next write tries again
            throw err;
        });
        return this.connecting;
    }

    async write(entry: DlqEntry): Promise<void> {
        await this.connect();
        await this.producer!.send({
            topic: this.dlqTopic,
            messages: [{ key: entry.message.key, value: entry.message.value, headers: dlqHeaders(entry) }],
        });
    }

    async disconnect(): Promise<void> {
        if (!this.connecting) return;
        await this.connecting.catch(() => {});
        await this.producer?.disconnect();
    }
}
