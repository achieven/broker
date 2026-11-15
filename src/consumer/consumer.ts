import { Consumer as KafkaConsumer, IHeaders, Producer as KafkaProducer } from 'kafkajs';
import { BrokerService } from '../broker.service';
import { IDEMPOTENCY_KEY_HEADER, IdempotencyStore } from '../idempotency/idempotency-store';
import { MongoCollectionLike, MongoIdempotencyOptions, mongoIdempotencyStore } from '../idempotency/mongo-idempotency-store';
import { MysqlIdempotencyOptions, SequelizeModelLike, mysqlIdempotencyStore } from '../idempotency/mysql-idempotency-store';

export interface MessageContext {
    topic: string;
    partition: number;
    offset: string;
    /** Present when the message carried the idempotency header. Write it with your document (unique-constrained). */
    idempotencyKey?: string;
}

export type MessageHandler = (data: any, ctx: MessageContext) => Promise<void>;

/**
 * Composable consumer:
 *   new Consumer(broker).withGroupId('g').withTopic('t').withReadCommitted()
 *       .withMongoIdempotencyCheck(collection).onMessage(handler).start()
 *
 * With an idempotency check enabled, for every message the consumer:
 *   1. reads the `idempotency-key` header; a missing key is a malformed message -> DLQ immediately;
 *   2. skips the handler if a document with that key already exists;
 *   3. runs the handler; if it fails on the DB unique constraint (a concurrent duplicate won the race)
 *      the message goes to the DLQ immediately, with no retries. Any other error is thrown so Kafka retries it.
 */
export class Consumer {
    private groupId?: string;
    private topic?: string;
    private handler?: MessageHandler;
    private readUncommitted = true;
    private fromBeginning = true;
    private store?: IdempotencyStore;
    private dlqTopic?: string;
    private consumer?: KafkaConsumer;
    private dlqProducer?: KafkaProducer;

    constructor(private readonly broker: BrokerService) {}

    withGroupId(groupId: string): this {
        this.groupId = groupId;
        return this;
    }

    withTopic(topic: string): this {
        this.topic = topic;
        return this;
    }

    /** Only see records from committed transactions (default is read_uncommitted). */
    withReadCommitted(): this {
        this.readUncommitted = false;
        return this;
    }

    withFromBeginning(fromBeginning: boolean): this {
        this.fromBeginning = fromBeginning;
        return this;
    }

    withMongoIdempotencyCheck(collection: MongoCollectionLike, options?: MongoIdempotencyOptions): this {
        return this.withIdempotencyStore(mongoIdempotencyStore(collection, options));
    }

    withMysqlIdempotencyCheck(model: SequelizeModelLike, options?: MysqlIdempotencyOptions): this {
        return this.withIdempotencyStore(mysqlIdempotencyStore(model, options));
    }

    withIdempotencyStore(store: IdempotencyStore): this {
        this.store = store;
        return this;
    }

    /** Override the dead-letter topic. Default: `<topic>.dlq`. */
    withDlqTopic(dlqTopic: string): this {
        this.dlqTopic = dlqTopic;
        return this;
    }

    onMessage(handler: MessageHandler): this {
        this.handler = handler;
        return this;
    }

    async start(): Promise<this> {
        if (!this.groupId || !this.topic || !this.handler) {
            throw new Error('Consumer needs withGroupId(), withTopic() and onMessage() before start()');
        }
        const topic = this.topic;
        const handler = this.handler;
        const store = this.store;
        const dlqTopic = this.dlqTopic ?? `${topic}.dlq`;

        if (store) {
            await store.ensureSchema(); // the unique constraint must exist before we consume anything
            this.dlqProducer = this.broker.kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
            await this.dlqProducer.connect();
        }

        this.consumer = this.broker.kafka.consumer({ groupId: this.groupId, readUncommitted: this.readUncommitted });
        await this.consumer.connect();
        await subscribeWhenTopicVisible(this.consumer, topic, this.fromBeginning);
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log('Received message:', message.value?.toString());
                const idempotencyKey = message.headers?.[IDEMPOTENCY_KEY_HEADER]?.toString();
                const ctx: MessageContext = { topic, partition, offset: message.offset, idempotencyKey };

                const sendToDlq = async (reason: string, error?: unknown) => {
                    console.error(`Sending message to ${dlqTopic}: ${reason}`, error ?? '');
                    // If this throws the message is retried (not lost); acks=all so a DLQ write is never silently dropped.
                    await this.dlqProducer!.send({
                        topic: dlqTopic,
                        acks: -1,
                        messages: [{
                            key: message.key,
                            value: message.value,
                            headers: {
                                ...(message.headers as IHeaders),
                                'dlq-reason': reason,
                                'dlq-error': String((error as Error)?.message ?? ''),
                                'dlq-source-topic': topic,
                                'dlq-source-partition': String(partition),
                                'dlq-source-offset': message.offset,
                            },
                        }],
                    });
                };

                if (store) {
                    if (!idempotencyKey) return sendToDlq('missing-idempotency-key');
                    if (await store.exists(idempotencyKey)) {
                        console.log(`Skipping already-processed message (idempotency key ${idempotencyKey})`);
                        return;
                    }
                }

                const value = message.value?.toString();
                if (!value) throw new Error('no value');
                const data = JSON.parse(value);
                try {
                    await handler(data, ctx);
                } catch (err) {
                    if (store?.isDuplicateKeyError(err)) {
                        return sendToDlq('duplicate-idempotency-key', err);
                    }
                    console.error('Error processing message:', err);
                    throw err;
                }
            },
        });
        return this;
    }

    async stop() {
        await this.consumer?.disconnect();
        await this.dlqProducer?.disconnect();
    }
}

/** A topic created moments ago may not be visible on every broker yet; retry instead of failing the consumer's startup. */
async function subscribeWhenTopicVisible(consumer: KafkaConsumer, topic: string, fromBeginning: boolean, timeoutMs = 30000) {
    const start = Date.now();
    for (;;) {
        try {
            return await consumer.subscribe({ topic, fromBeginning });
        } catch (err: any) {
            if (err?.type !== 'UNKNOWN_TOPIC_OR_PARTITION' || Date.now() - start > timeoutMs) throw err;
            await new Promise((resolve) => setTimeout(resolve, 300));
        }
    }
}
