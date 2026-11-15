import { KafkaJS } from '@confluentinc/kafka-javascript';
import { BrokerService } from '../broker.service';
import { IDEMPOTENCY_KEY_HEADER, IdempotencyStore } from '../idempotency/idempotency-store';
import { MongoCollectionLike, MongoIdempotencyOptions, mongoIdempotencyStore } from '../idempotency/mongo-idempotency-store';
import { MysqlIdempotencyOptions, SequelizeModelLike, mysqlIdempotencyStore } from '../idempotency/mysql-idempotency-store';
import { instanceId, liveTransactionalIds } from '../producer/transactional-id';
import { Classifier, ErrorClass, genericClassifier, markerClass } from './classify/error-class';
import { mongoClassifier } from './classify/mongo-classifier';
import { mysqlClassifier } from './classify/mysql-classifier';
import { DlqWriter, PlainDlqWriter, TransactionalDlqWriter } from './dlq';
import { Gate } from './gate';
import { DEFAULT_RETRY, backoffDelay } from './retry';
import {
    ConsumedMessage, ConsumerEvent, ConsumerEventListener, IdempotencyPolicy, MessageMeta, RecordHandler, RetryPolicy,
} from './types';

export * from './types';

export interface MessageContext {
    topic: string;
    partition: number;
    offset: string;
    /** The resolved idempotency key. Write it with your document (unique-constrained). */
    idempotencyKey?: string;
}

export type MessageHandler = (data: any, ctx: MessageContext) => Promise<void>;

/** Thrown internally for a message that has no idempotency key while the policy requires one. */
class MissingIdempotencyKeyError extends Error {
    readonly nonRetryable = true;
    constructor() { super('message has no idempotency key'); }
}

interface ResolvedPolicy extends IdempotencyPolicy {
    onDuplicate: 'ack' | 'dlq';
    isDuplicate?: (err: unknown) => boolean;
    /** The deprecated store API: header-only key, a missing key is dead-lettered, a duplicate is dead-lettered. */
    legacy: boolean;
}

type Outcome = 'handled' | 'already-done' | 'duplicate';
type Verdict =
    | { kind: 'done'; outcome: Outcome; attempts: number }
    | { kind: 'dlq'; errorClass: string; reason: string; error?: unknown }
    | { kind: 'stopped' };

/**
 * Composable consumer:
 *   new Consumer(broker).withGroupId('g').withTopic('t').withReadCommitted().onMessage(handler).start()
 *
 * Two processing modes, same builder:
 *
 * DEFAULT (unchanged): the client auto-commits offsets; an error thrown by the handler is retried by the client;
 * with a legacy idempotency store (withMongoIdempotencyCheck / withMysqlIdempotencyCheck / withIdempotencyStore)
 * missing keys and duplicate-key failures go to `<topic>.dlq`, and ensureSchema() runs at start().
 *
 * STRICT (`withStrictProcessing()`), for pipelines where losing or double-applying a message is not acceptable:
 *   - autoCommit is off; after the handler returned (your DB transaction committed) offset+1 of THAT message is committed;
 *   - failures are classified (see `ErrorClass`, `withClassifier`, `withMysqlClassifier`, `withMongoClassifier`):
 *     permanent -> DLQ now; transient-message / unknown -> bounded retries, then DLQ; transient-infra -> retried until it
 *     succeeds (or the consumer stops), never dead-lettered; duplicate -> per the idempotency policy;
 *   - the DLQ record and the source offset are written in one Kafka transaction (all or nothing).
 *
 * In both modes: tombstones/empty values are dead-lettered instead of throwing, `withGate` halts processing fail-closed,
 * `withPartitionConcurrency(n)` processes n partitions in parallel (order is kept per partition), `onEvent` observes.
 */
export class Consumer {
    private groupId?: string;
    private topic?: string;
    private handler?: RecordHandler;
    private readUncommitted = true;
    private fromBeginning = true;
    private store?: IdempotencyStore;
    private policy?: IdempotencyPolicy;
    private dlqTopic?: string;
    private strict = false;
    private classifiers: Classifier[] = [];
    private retry?: RetryPolicy;
    private stage?: string;
    private concurrency?: number;
    private gateRead?: () => Promise<boolean>;
    private gatePollMs?: number;
    private listeners: ConsumerEventListener[] = [];

    private consumer?: KafkaJS.Consumer;
    private dlq?: DlqWriter;
    private gate?: Gate;
    private transactionalId?: string;
    private resolved?: ResolvedPolicy;
    private retryPolicy: Required<RetryPolicy> = DEFAULT_RETRY;
    private stopped = false;
    private sleepers = new Set<() => void>();

    constructor(private readonly broker: BrokerService) {}

    withGroupId(groupId: string): this {
        this.groupId = groupId;
        return this;
    }

    withTopic(topic: string): this {
        this.topic = topic;
        return this;
    }

    /** Only see records from committed transactions (default is read_uncommitted). Use it behind transactional sources such as Debezium. */
    withReadCommitted(): this {
        this.readUncommitted = false;
        return this;
    }

    withFromBeginning(fromBeginning: boolean): this {
        this.fromBeginning = fromBeginning;
        return this;
    }

    /**
     * Idempotency policy: where the key comes from, the pre-check, and what a duplicate means.
     * Never touches the schema: manage your unique constraint yourself (the service's DB user needs no DDL rights).
     */
    withIdempotency(policy: IdempotencyPolicy): this {
        this.policy = policy;
        return this;
    }

    /** Mongo store. Creates the unique index at start() (needs createIndex rights); prefer withIdempotency() if you manage the schema. */
    withMongoIdempotencyCheck(collection: MongoCollectionLike, options?: MongoIdempotencyOptions): this {
        return this.withIdempotencyStore(mongoIdempotencyStore(collection, options));
    }

    /** MySQL store. Runs model.sync() and adds the unique index at start() (needs DDL rights); prefer withIdempotency() if you manage the schema. */
    withMysqlIdempotencyCheck(model: SequelizeModelLike, options?: MysqlIdempotencyOptions): this {
        return this.withIdempotencyStore(mysqlIdempotencyStore(model, options));
    }

    /** Legacy store API: calls store.ensureSchema() at start(); a missing key and a duplicate-key failure go to the DLQ. */
    withIdempotencyStore(store: IdempotencyStore): this {
        this.store = store;
        return this;
    }

    /** Override the dead-letter topic. Default: `<topic>.dlq`. */
    withDlqTopic(dlqTopic: string): this {
        this.dlqTopic = dlqTopic;
        return this;
    }

    /** Manual per-message commits, failure classification and the transactional DLQ. See the class comment. */
    withStrictProcessing(): this {
        this.strict = true;
        return this;
    }

    /**
     * Custom classifier, consulted before the built-in ones (calls accumulate, earlier calls win). The generic baseline
     * (markers, ZodError/SyntaxError, network and redis outages) is always the last one. Explicit markers on the error
     * (`nonRetryable: true`, `retriable: true`) override every classifier.
     */
    withClassifier(classify: Classifier): this {
        this.classifiers.push(classify);
        return this;
    }

    /** MySQL error numbers (deadlock, data too long, connection lost, ...). See classify/mysql-classifier.ts. */
    withMysqlClassifier(): this {
        return this.withClassifier(mysqlClassifier);
    }

    /** MongoDB error codes and labels (write conflict, not-primary, duplicate key, ...). See classify/mongo-classifier.ts. */
    withMongoClassifier(): this {
        return this.withClassifier(mongoClassifier);
    }

    /** Strict mode only: attempts and backoff for transient-message / unknown / transient-infra failures. */
    withRetryPolicy(policy: RetryPolicy): this {
        this.retry = policy;
        return this;
    }

    /** `x-stage` header on dead-lettered messages. Default: the group id. */
    withStage(stage: string): this {
        this.stage = stage;
        return this;
    }

    /** How many partitions are processed at the same time (partitionsConsumedConcurrently). Order is kept within a partition. */
    withPartitionConcurrency(partitions: number): this {
        if (!Number.isInteger(partitions) || partitions < 1) throw new Error('withPartitionConcurrency needs an integer >= 1');
        this.concurrency = partitions;
        return this;
    }

    /**
     * HALT gate, fail closed. `true` means halted: no message is processed, no offset is committed, heartbeats keep the
     * session alive; processing resumes when the gate returns false. Read once at start(), then polled (default 1 s).
     * A rejected promise counts as halted.
     */
    withGate(isHalted: () => Promise<boolean>, options: { pollMs?: number } = {}): this {
        this.gateRead = isHalted;
        this.gatePollMs = options.pollMs;
        return this;
    }

    /** Observability: 'processed' | 'dlq' | 'retry'. A throwing listener is ignored. */
    onEvent(listener: ConsumerEventListener): this {
        this.listeners.push(listener);
        return this;
    }

    /** Handler with the parsed JSON and a small context. A malformed JSON value is a permanent failure. */
    onMessage(handler: MessageHandler): this {
        this.handler = async (msg) => handler(msg.json(), {
            topic: msg.meta.topic,
            partition: msg.meta.partition,
            offset: msg.meta.offset,
            idempotencyKey: msg.idempotencyKey,
        });
        return this;
    }

    /** Handler with the raw record: `{ value: Buffer, key, headers, meta: { topic, partition, offset }, json(), header(name) }`. */
    onRecord(handler: RecordHandler): this {
        this.handler = handler;
        return this;
    }

    async start(): Promise<this> {
        if (!this.groupId || !this.topic || !this.handler) {
            throw new Error('Consumer needs withGroupId(), withTopic() and onMessage()/onRecord() before start()');
        }
        if (this.store && this.policy) throw new Error('Use either withIdempotency() or a legacy store (with*IdempotencyCheck), not both');
        if (this.retry && !this.strict) throw new Error('withRetryPolicy() only applies to withStrictProcessing()');

        const groupId = this.groupId;
        const topic = this.topic;
        const dlqTopic = this.dlqTopic ?? `${topic}.dlq`;
        this.retryPolicy = { ...DEFAULT_RETRY, ...this.retry };
        this.resolved = this.resolvePolicy();

        if (this.store) await this.store.ensureSchema(); // legacy store only: the unique constraint must exist before we consume

        this.stopped = false;
        if (this.strict) {
            this.transactionalId = `dlq-${groupId}-${topic}-${instanceId()}`;
            if (liveTransactionalIds.has(this.transactionalId)) {
                throw new Error(`Transactional id "${this.transactionalId}" is already used by another producer in this process`);
            }
            liveTransactionalIds.add(this.transactionalId);
            const writer = new TransactionalDlqWriter(this.broker, dlqTopic, this.transactionalId);
            this.dlq = writer;
            try {
                await writer.connect();
            } catch (err) {
                this.releaseTransactionalId();
                throw err;
            }
        } else {
            const writer = new PlainDlqWriter(this.broker, dlqTopic);
            this.dlq = writer;
            if (this.store) await writer.connect(); // as before: fail at start, not at the first dead letter
        }

        if (this.gateRead) {
            this.gate = new Gate(this.gateRead, this.gatePollMs);
            await this.gate.start();
        }

        this.consumer = this.broker.kafka.consumer({
            kafkaJS: {
                groupId,
                readUncommitted: this.readUncommitted,
                fromBeginning: this.fromBeginning,
                ...(this.strict ? { autoCommit: false } : {}),
            },
        });
        if (this.dlq instanceof TransactionalDlqWriter) this.dlq.attachConsumer(this.consumer);
        await this.consumer.connect();
        await subscribeWhenTopicVisible(this.consumer, topic);
        await this.consumer.run({
            ...(this.concurrency ? { partitionsConsumedConcurrently: this.concurrency } : {}),
            eachMessage: (payload) => this.handleMessage(payload, dlqTopic),
        });
        return this;
    }

    async stop() {
        this.stopped = true;
        this.gate?.stop();
        [...this.sleepers].forEach((wake) => wake()); // interrupt backoffs and the halted wait; their offsets stay uncommitted
        try {
            await this.consumer?.disconnect();
            await this.dlq?.disconnect();
        } finally {
            this.releaseTransactionalId();
        }
    }

    // ---------------------------------------------------------------- per message

    private async handleMessage({ topic, partition, message, heartbeat }: KafkaJS.EachMessagePayload, dlqTopic: string): Promise<void> {
        if (this.stopped) return;
        console.log('Received message:', message.value?.toString());
        if (!(await this.waitWhileHalted(heartbeat))) return;

        const meta: MessageMeta = { topic, partition, offset: message.offset };

        // A tombstone or empty value can never be processed. Dead-letter it for manual investigation rather than throwing,
        // which would block the partition forever.
        if (!message.value || message.value.length === 0) {
            return this.finish(meta, message, await this.deadLetter(meta, message, dlqTopic, heartbeat,
                { errorClass: 'tombstone', reason: 'tombstone' }), heartbeat);
        }

        const record = toConsumedMessage(message, meta);
        const verdict = this.strict ? await this.runStrict(record, heartbeat) : await this.runDefault(record);
        if (verdict.kind === 'done') {
            if (this.strict) await this.commit(meta);
            this.emit({ type: 'processed', meta, outcome: verdict.outcome, attempts: verdict.attempts, idempotencyKey: record.idempotencyKey });
            return;
        }
        if (verdict.kind === 'stopped') return;
        return this.finish(meta, message, await this.deadLetter(meta, message, dlqTopic, heartbeat, verdict, record), heartbeat);
    }

    private finish(_meta: MessageMeta, _message: KafkaJS.KafkaMessage, _result: 'dlq' | 'stopped', _heartbeat: () => Promise<void>): void {
        // dead-lettering already emitted its event; in strict mode the offset was committed inside the DLQ transaction
    }

    /** Default mode: duplicates and missing keys are handled, everything else is rethrown for the client to retry. */
    private async runDefault(record: ConsumedMessage): Promise<Verdict> {
        try {
            return { kind: 'done', outcome: await this.attempt(record), attempts: 1 };
        } catch (err) {
            if (err instanceof MissingIdempotencyKeyError) {
                return { kind: 'dlq', errorClass: 'permanent', reason: 'missing-idempotency-key', error: err };
            }
            if (this.resolved && this.classify(err) === 'duplicate') {
                return this.onDuplicate(err);
            }
            console.error('Error processing message:', err);
            throw err;
        }
    }

    /** Strict mode: classify, retry, and decide. Never throws for a processing failure. */
    private async runStrict(record: ConsumedMessage, heartbeat: () => Promise<void>): Promise<Verdict> {
        const { meta } = record;
        let attempts = 0;
        let infraAttempts = 0;
        for (;;) {
            if (this.stopped) return { kind: 'stopped' };
            attempts++;
            try {
                return { kind: 'done', outcome: await this.attempt(record), attempts };
            } catch (err) {
                const errorClass = this.classify(err);
                const reason = err instanceof MissingIdempotencyKeyError ? 'missing-idempotency-key' : undefined;
                switch (errorClass) {
                    case 'duplicate':
                        return this.onDuplicate(err);
                    case 'permanent':
                        return { kind: 'dlq', errorClass: 'permanent', reason: reason ?? 'permanent-error', error: err };
                    case 'transient-infra': {
                        // a dependency is down: waiting is the only correct thing to do; the offset stays uncommitted
                        infraAttempts++;
                        attempts--; // not the message's fault, doesn't count against maxAttempts
                        const delayMs = backoffDelay(infraAttempts, this.retryPolicy);
                        this.emit({ type: 'retry', meta, errorClass, attempt: infraAttempts, delayMs, error: errorText(err), phase: 'handler' });
                        if (!(await this.sleep(delayMs, heartbeat))) return { kind: 'stopped' };
                        break;
                    }
                    default: { // transient-message | unknown
                        if (attempts >= this.retryPolicy.maxAttempts) {
                            return { kind: 'dlq', errorClass: 'exhausted', reason: 'retries-exhausted', error: err };
                        }
                        const delayMs = backoffDelay(attempts, this.retryPolicy);
                        this.emit({ type: 'retry', meta, errorClass, attempt: attempts, delayMs, error: errorText(err), phase: 'handler' });
                        if (!(await this.sleep(delayMs, heartbeat))) return { kind: 'stopped' };
                    }
                }
            }
        }
    }

    /** One try: resolve the key, pre-check, run the handler. */
    private async attempt(record: ConsumedMessage): Promise<Outcome> {
        const policy = this.resolved;
        if (policy) {
            record.idempotencyKey = this.resolveKey(record, policy);
            if (!record.idempotencyKey) {
                if (policy.requireKey) throw new MissingIdempotencyKeyError();
            } else if (policy.alreadyDone && (await policy.alreadyDone(record.idempotencyKey, record))) {
                console.log(`Skipping already-processed message (idempotency key ${record.idempotencyKey})`);
                return 'already-done';
            }
        } else {
            record.idempotencyKey = record.header(IDEMPOTENCY_KEY_HEADER);
        }
        await this.handler!(record);
        return 'handled';
    }

    private onDuplicate(err: unknown): Verdict {
        // Without a policy nobody opted in to "a duplicate means done": dead-letter it, never lose it silently.
        if (!this.resolved) return { kind: 'dlq', errorClass: 'duplicate', reason: 'duplicate-key', error: err };
        if (this.resolved.onDuplicate === 'dlq') {
            return { kind: 'dlq', errorClass: 'duplicate', reason: 'duplicate-idempotency-key', error: err };
        }
        return { kind: 'done', outcome: 'duplicate', attempts: 1 };
    }

    private classify(err: unknown): ErrorClass {
        const marker = markerClass(err);
        if (marker) return marker;
        if (this.resolved?.isDuplicate?.(err)) return 'duplicate';
        for (const classify of [...this.classifiers, genericClassifier]) {
            try {
                const result = classify(err);
                if (result !== 'unknown') return result;
            } catch { /* a broken classifier must not mask the original error */ }
        }
        return 'unknown';
    }

    private resolveKey(record: ConsumedMessage, policy: ResolvedPolicy): string | undefined {
        const header = record.header(IDEMPOTENCY_KEY_HEADER);
        if (policy.legacy) return header;
        const fromFn = policy.keyFrom?.(record);
        return (fromFn ? String(fromFn) : undefined) ?? header ?? record.key?.toString('utf8');
    }

    private resolvePolicy(): ResolvedPolicy | undefined {
        if (this.store) {
            const store = this.store;
            return { alreadyDone: (key) => store.exists(key), onDuplicate: 'dlq', requireKey: true, isDuplicate: (e) => store.isDuplicateKeyError(e), legacy: true };
        }
        if (this.policy) return { ...this.policy, onDuplicate: this.policy.onDuplicate ?? 'ack', legacy: false };
        return undefined;
    }

    // ---------------------------------------------------------------- offsets and DLQ

    /** Commits offset+1 of this message only, and only after its handler finished. Failures propagate: the client redelivers. */
    private async commit({ topic, partition, offset }: MessageMeta): Promise<void> {
        await this.consumer!.commitOffsets([{ topic, partition, offset: (BigInt(offset) + 1n).toString() }]);
    }

    private async deadLetter(
        meta: MessageMeta,
        message: KafkaJS.KafkaMessage,
        dlqTopic: string,
        heartbeat: () => Promise<void>,
        verdict: { errorClass: string; reason: string; error?: unknown },
        record?: ConsumedMessage,
    ): Promise<'dlq' | 'stopped'> {
        const entry = { meta, message, stage: this.stage ?? this.groupId!, errorClass: verdict.errorClass, reason: verdict.reason, error: verdict.error };
        console.error(`Sending message to ${dlqTopic}: ${verdict.reason}`, verdict.error ?? '');
        for (let attempt = 1; ; attempt++) {
            if (this.stopped) return 'stopped';
            try {
                await this.dlq!.write(entry);
                this.emit({
                    type: 'dlq', meta, dlqTopic, errorClass: verdict.errorClass, reason: verdict.reason,
                    error: verdict.error === undefined ? undefined : errorText(verdict.error),
                });
                void record;
                return 'dlq';
            } catch (err) {
                // Default mode: as before, throw and let the client redeliver. Strict mode: nothing was committed (the
                // transaction aborted), so keep trying until the DLQ is reachable; losing the message is not an option.
                if (!this.strict) throw err;
                const delayMs = backoffDelay(attempt, this.retryPolicy);
                this.emit({ type: 'retry', meta, errorClass: 'transient-infra', attempt, delayMs, error: errorText(err), phase: 'dlq' });
                if (!(await this.sleep(delayMs, heartbeat))) return 'stopped';
            }
        }
    }

    // ---------------------------------------------------------------- waiting

    /** While the gate says halted: don't process, keep the session alive. False if the consumer was stopped meanwhile. */
    private async waitWhileHalted(heartbeat: () => Promise<void>): Promise<boolean> {
        while (this.gate?.isHalted && !this.stopped) {
            await heartbeat();
            await this.nap(500);
        }
        return !this.stopped;
    }

    /** Sleeps `ms` in slices, heartbeating between them. False if the consumer was stopped meanwhile. */
    private async sleep(ms: number, heartbeat: () => Promise<void>): Promise<boolean> {
        const end = Date.now() + ms;
        while (!this.stopped) {
            const left = end - Date.now();
            if (left <= 0) return true;
            await this.nap(Math.min(left, 1000));
            if (!this.stopped) await heartbeat();
        }
        return false;
    }

    /** A timer that stop() can cut short. */
    private nap(ms: number): Promise<void> {
        return new Promise((resolve) => {
            const done = () => { clearTimeout(timer); this.sleepers.delete(done); resolve(); };
            const timer = setTimeout(done, ms);
            this.sleepers.add(done);
        });
    }

    private emit(event: ConsumerEvent) {
        for (const listener of this.listeners) {
            try { listener(event); } catch { /* observers must never break processing */ }
        }
    }

    private releaseTransactionalId() {
        if (this.transactionalId) liveTransactionalIds.delete(this.transactionalId);
        this.transactionalId = undefined;
    }
}

function isUnknownTopic(err: any): boolean {
    return err?.type === 'UNKNOWN_TOPIC_OR_PARTITION' || err?.code === KafkaJS.ErrorCodes.ERR_UNKNOWN_TOPIC_OR_PART;
}

function errorText(err: unknown): string {
    return String((err as Error)?.message ?? err);
}

function toConsumedMessage(message: KafkaJS.KafkaMessage, meta: MessageMeta): ConsumedMessage {
    const headers = message.headers ?? {};
    let parsed: { value: any } | undefined;
    return {
        value: message.value as Buffer,
        key: message.key,
        headers,
        meta,
        json() {
            parsed ??= { value: JSON.parse((message.value as Buffer).toString('utf8')) };
            return parsed.value;
        },
        header(name) {
            const h = headers[name];
            const first = Array.isArray(h) ? h[0] : h;
            return first == null ? undefined : first.toString();
        },
    };
}

/** A topic created moments ago may not be visible on every broker yet; retry instead of failing the consumer's startup. */
async function subscribeWhenTopicVisible(consumer: KafkaJS.Consumer, topic: string, timeoutMs = 30000) {
    const start = Date.now();
    for (;;) {
        try {
            return await consumer.subscribe({ topic });
        } catch (err: any) {
            if (!isUnknownTopic(err) || Date.now() - start > timeoutMs) throw err;
            await new Promise((resolve) => setTimeout(resolve, 300));
        }
    }
}
