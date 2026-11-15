import { IHeaders } from 'kafkajs';
import { ErrorClass } from './classify/error-class';

export interface MessageMeta {
    topic: string;
    partition: number;
    offset: string;
}

/** What `onRecord()` handlers receive: the raw Kafka record plus conveniences. */
export interface ConsumedMessage {
    /** Raw value. Never null/empty here: tombstones and empty values are dead-lettered before the handler runs. */
    value: Buffer;
    key: Buffer | null;
    headers: IHeaders;
    meta: MessageMeta;
    /** The idempotency key resolved by the idempotency policy (undefined when none is configured or found). */
    idempotencyKey?: string;
    /** `JSON.parse(value)`, memoised. Throws SyntaxError on malformed JSON, which is classified as permanent. */
    json<T = any>(): T;
    /** A header decoded as a UTF-8 string. */
    header(name: string): string | undefined;
}

export type RecordHandler = (message: ConsumedMessage) => Promise<void>;

export interface IdempotencyPolicy {
    /**
     * Where the key comes from. Tried first; when it returns nothing the `idempotency-key` header is used, then the Kafka
     * message key. Note: with Debezium outbox topics the Kafka key is usually the aggregate id and is shared by many
     * events, so return a per-event id here (e.g. a payload field) unless `alreadyDone` is itself stage-aware.
     */
    keyFrom?(message: ConsumedMessage): string | undefined | null;
    /**
     * Pre-check, an optimisation only: true means the work for this key is already done, so the handler is skipped and the
     * message acknowledged. Typically stage-specific ("status is already past this stage"). The real guarantee against
     * concurrent duplicates is a unique constraint in the database.
     */
    alreadyDone?(key: string, message: ConsumedMessage): Promise<boolean> | boolean;
    /**
     * What a duplicate-key failure (error class 'duplicate') means. 'ack' (default): another worker already did the work,
     * acknowledge the message. 'dlq': dead-letter it.
     */
    onDuplicate?: 'ack' | 'dlq';
    /** Dead-letter messages without a key (default false: they are processed, without pre-check). */
    requireKey?: boolean;
}

export interface RetryPolicy {
    /** Total handler invocations for transient-message / unknown errors before the message is dead-lettered. Default 5. */
    maxAttempts?: number;
    /** First backoff, doubled on every attempt, jittered. Default 100 ms. */
    baseDelayMs?: number;
    /** Backoff ceiling. Default 10 000 ms. */
    maxDelayMs?: number;
}

export type ConsumerEvent =
    | { type: 'processed'; meta: MessageMeta; outcome: 'handled' | 'already-done' | 'duplicate'; attempts: number; idempotencyKey?: string }
    | { type: 'dlq'; meta: MessageMeta; dlqTopic: string; errorClass: string; reason: string; error?: string }
    | { type: 'retry'; meta: MessageMeta; errorClass: ErrorClass; attempt: number; delayMs: number; error: string; phase: 'handler' | 'dlq' };

export type ConsumerEventListener = (event: ConsumerEvent) => void;
