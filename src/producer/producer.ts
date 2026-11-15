import { randomUUID } from 'node:crypto';
import { CompressionTypes, Producer as KafkaProducer, ProducerConfig } from 'kafkajs';
import { BrokerService } from '../broker.service';
import { IDEMPOTENCY_KEY_HEADER } from '../idempotency/idempotency-store';
import { DurableTopicService } from '../topic/durable-topic.service';
import { TopicService } from '../topic/topic.service';
import { instanceId, liveTransactionalIds } from './transactional-id';

export interface ProduceOptions {
    /** Reuse an existing correlation id (e.g. when forwarding an event). A fresh UUID is generated when omitted. */
    idempotencyKey?: string;
}

export interface TransactionalProduceOptions extends ProduceOptions {
    /** Write to a different topic than the producer's own, atomically with the rest of the transaction. */
    topic?: string | TopicService;
}

export interface TransactionOptions {
    /** Override the generated transactional id. Must be stable for this logical producer and unique per running instance. */
    transactionalId?: string;
}

/** Handed to the callback of `producer.transaction()`. Nothing is visible to read_committed consumers until it returns. */
export interface ProducerTransaction {
    produce(message: any, options?: TransactionalProduceOptions): Promise<string>;
}

/**
 * Composable producer:  `new Producer(broker).withDurability(ordersTopic)`  or  `new Producer(broker).withTopic('x')`.
 * Every message gets an idempotency key header (`idempotency-key`) that consumers use to dedupe against the database.
 */
export class Producer {
    private topic?: string | TopicService;
    private config: ProducerConfig = {};
    private acks: -1 | 0 | 1 = 1;
    private sendTimeout?: number;
    private transactional?: TransactionOptions;
    private activeTransactionalId?: string;
    private producer?: KafkaProducer;
    private connected = false;
    private queue: Promise<unknown> = Promise.resolve(); // a kafkajs producer runs one transaction at a time

    constructor(private readonly broker: BrokerService) {}

    withTopic(topic: string | TopicService): this {
        this.assertNotConnected();
        this.topic = topic;
        return this;
    }

    /**
     * Idempotent producer, works with any topic (string, plain or durable): a retried send (e.g. after a lost
     * acknowledgement) is written exactly once, in order. Implies acks=all, maxInFlightRequests=1 and persistent retries.
     */
    withIdempotence(): this {
        this.assertNotConnected();
        this.acks = -1;
        this.sendTimeout = 30000;
        this.config = {
            ...this.config,
            idempotent: true,
            maxInFlightRequests: 1,
            retry: { retries: 20, initialRetryTime: 300, maxRetryTime: 30000 },
        };
        return this;
    }

    /**
     * Maximally durable first-hop producer: withIdempotence() + it writes ONLY to a DurableTopicService
     * (type-enforced, replication/min-ISR validated), and never auto-creates topics.
     */
    withDurability(topic: DurableTopicService): this {
        this.withIdempotence();
        this.topic = topic;
        this.config = { ...this.config, allowAutoTopicCreation: false }; // a typo'd/missing topic must fail loudly
        return this;
    }

    /**
     * Transactional producer (implies withIdempotence()). `produce()` then commits each message in its own
     * transaction, and `transaction(fn)` writes several messages, possibly to several topics, atomically:
     * read_committed consumers see all of them or none.
     *
     * The transactional id is generated here, deterministically: `tx-<topic>-<instance>`, where the instance is
     * $PRODUCER_INSTANCE_ID, else $HOSTNAME, else the OS hostname. So the same logical producer gets the same id
     * on every restart (the broker then fences its zombie and cleans up its half-finished transaction), while
     * different instances get different ids. Replicas therefore need distinct hostnames / PRODUCER_INSTANCE_ID
     * (true for Kubernetes pods; stable across restarts for StatefulSets).
     */
    withTransactions(options: TransactionOptions = {}): this {
        this.withIdempotence();
        this.transactional = options;
        return this;
    }

    /** The transactional id in use (available after connect()). */
    get transactionalId(): string | undefined {
        return this.activeTransactionalId;
    }

    async connect(): Promise<this> {
        if (!this.topic) throw new Error('Producer has no topic: call withTopic() or withDurability()');
        if (typeof this.topic !== 'string') await this.topic.ensure(); // create + validate before connecting

        let config = this.config;
        if (this.transactional) {
            const id = this.transactional.transactionalId ?? defaultTransactionalId(this.topicName);
            if (liveTransactionalIds.has(id)) {
                throw new Error(
                    `Transactional id "${id}" is already used by another producer in this process; ` +
                    `two producers with the same id would fence each other. Pass a distinct { transactionalId }.`,
                );
            }
            liveTransactionalIds.add(id);
            this.activeTransactionalId = id;
            config = { ...config, transactionalId: id };
        }

        try {
            this.producer = this.broker.kafka.producer(config);
            await this.producer.connect();
        } catch (err) {
            this.releaseTransactionalId();
            throw err;
        }
        this.connected = true;
        return this;
    }

    async disconnect() {
        await this.producer?.disconnect();
        this.connected = false;
        this.releaseTransactionalId();
    }

    /**
     * Sends the message and returns the idempotency key it was tagged with. Throws if the send was not acknowledged.
     * On a transactional producer the message is committed in its own transaction.
     */
    async produce(message: any, options: ProduceOptions = {}): Promise<string> {
        const { producer, topic } = this.assertConnected();
        const idempotencyKey = options.idempotencyKey ?? randomUUID();
        if (this.transactional) {
            await this.transaction(async (tx) => { await tx.produce(message, { idempotencyKey }); });
            return idempotencyKey;
        }
        await producer.send({
            topic: topicName(topic),
            acks: this.acks,
            ...(this.sendTimeout ? { timeout: this.sendTimeout } : {}),
            compression: CompressionTypes.None,
            messages: [this.toRecord(message, idempotencyKey)],
        });
        return idempotencyKey;
    }

    /**
     * Atomic group of messages: committed when `fn` resolves, aborted (nothing becomes visible to read_committed
     * consumers) when it throws, in which case the error is rethrown. Transactions on one producer run one at a time.
     */
    async transaction<T = void>(fn: (tx: ProducerTransaction) => Promise<T>): Promise<T> {
        const { producer, topic } = this.assertConnected();
        if (!this.transactional) throw new Error('Producer is not transactional: call withTransactions() before connect()');

        const run = async (): Promise<T> => {
            const tx = await producer.transaction();
            try {
                const result = await fn({
                    produce: async (message, options = {}) => {
                        const idempotencyKey = options.idempotencyKey ?? randomUUID();
                        const target = options.topic ?? topic;
                        if (typeof target !== 'string') await target.ensure();
                        await tx.send({
                            topic: topicName(target),
                            acks: -1,
                            compression: CompressionTypes.None,
                            messages: [this.toRecord(message, idempotencyKey)],
                        });
                        return idempotencyKey;
                    },
                });
                await tx.commit();
                return result;
            } catch (err) {
                await tx.abort().catch(() => { /* already aborted/fenced: surface the original error */ });
                throw err;
            }
        };
        const result = this.queue.then(run, run);
        this.queue = result.catch(() => undefined);
        return result;
    }

    private toRecord(message: any, idempotencyKey: string) {
        return { value: JSON.stringify(message), headers: { [IDEMPOTENCY_KEY_HEADER]: idempotencyKey } };
    }

    private get topicName(): string {
        return topicName(this.topic!);
    }

    private releaseTransactionalId() {
        if (this.activeTransactionalId) liveTransactionalIds.delete(this.activeTransactionalId);
        this.activeTransactionalId = undefined;
    }

    private assertConnected() {
        if (!this.connected || !this.producer || !this.topic) throw new Error('Producer is not connected: call connect() first');
        return { producer: this.producer, topic: this.topic };
    }

    private assertNotConnected() {
        if (this.connected) throw new Error('Producer is already connected; configure it before connect()');
    }
}

function topicName(topic: string | TopicService): string {
    return typeof topic === 'string' ? topic : topic.name;
}

function defaultTransactionalId(topic: string): string {
    return `tx-${topic}-${instanceId()}`;
}
