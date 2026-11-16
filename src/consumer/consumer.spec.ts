import { Consumer } from './consumer';
import { Producer } from '../producer/producer';
import { DurableTopicService } from '../topic/durable-topic.service';
import { mongoIdempotencyStore } from '../idempotency/mongo-idempotency-store';
import { mysqlIdempotencyStore } from '../idempotency/mysql-idempotency-store';

function fakeBroker() {
    const sent: any[] = [];
    let eachMessage: any;
    const kafka: any = {
        producer: jest.fn(() => ({ connect: jest.fn(), disconnect: jest.fn(), send: jest.fn(async (r) => { sent.push(r); }) })),
        consumer: jest.fn(() => ({
            connect: jest.fn(), disconnect: jest.fn(), subscribe: jest.fn(), assignment: jest.fn(() => [{ topic: 't', partition: 0 }]),
            run: jest.fn(async (o) => { eachMessage = o.eachMessage; }),
        })),
    };
    return { broker: { kafka } as any, sent, deliver: (headers: any = { 'idempotency-key': Buffer.from('k1') }) =>
        eachMessage({ topic: 't', partition: 0, message: { value: Buffer.from('{"a":1}'), offset: '5', headers } }) };
}

describe('Producer', () => {
    it('always tags messages with an idempotency key and uses acks=all when durable', async () => {
        const { broker, sent } = fakeBroker();
        class T extends DurableTopicService {}
        const topic = new (T as any)(broker, 'orders');
        topic.ensure = jest.fn();
        const producer = await new Producer(broker).withDurability(topic).connect();
        const key = await producer.produce({ x: 1 });
        // acks is a producer-level setting in the Confluent client (send() rejects it)
        expect(broker.kafka.producer.mock.calls[0][0].kafkaJS.acks).toBe(-1);
        expect(sent[0].acks).toBeUndefined();
        expect(sent[0].messages[0].headers['idempotency-key']).toBe(key);
        expect(await producer.produce({ x: 1 }, { idempotencyKey: 'abc' })).toBe('abc');
    });
});

describe('Consumer idempotency', () => {
    const build = (store: any, handler: any) => {
        const f = fakeBroker();
        const consumer = new Consumer(f.broker).withGroupId('g').withTopic('t').withReadCommitted()
            .withIdempotencyStore(store).onMessage(handler);
        return { ...f, consumer };
    };
    const store = (over: any = {}) => ({ ensureSchema: jest.fn(), exists: jest.fn(async () => false), isDuplicateKeyError: () => false, ...over });

    it('skips the handler when the key already exists', async () => {
        const handler = jest.fn();
        const { consumer, deliver, sent } = build(store({ exists: async () => true }), handler);
        await consumer.start(); await deliver();
        expect(handler).not.toHaveBeenCalled(); expect(sent).toHaveLength(0);
    });

    it('runs the handler with the key when new', async () => {
        const handler = jest.fn();
        const { consumer, deliver } = build(store(), handler);
        await consumer.start(); await deliver();
        expect(handler).toHaveBeenCalledWith({ a: 1 }, expect.objectContaining({ idempotencyKey: 'k1' }));
    });

    it('sends unique-constraint failures to the DLQ without throwing', async () => {
        const dup = new Error('dup');
        const { consumer, deliver, sent } = build(store({ isDuplicateKeyError: (e: any) => e === dup }), jest.fn().mockRejectedValue(dup));
        await consumer.start(); await deliver();
        expect(sent[0].topic).toBe('t.dlq');
        expect(sent[0].messages[0].headers['dlq-reason']).toBe('duplicate-idempotency-key');
    });

    it('rethrows other errors so Kafka retries', async () => {
        const { consumer, deliver, sent } = build(store(), jest.fn().mockRejectedValue(new Error('boom')));
        await consumer.start();
        await expect(deliver()).rejects.toThrow('boom'); expect(sent).toHaveLength(0);
    });

    it('sends messages without a key to the DLQ', async () => {
        const handler = jest.fn();
        const { consumer, deliver, sent } = build(store(), handler);
        await consumer.start(); await deliver({});
        expect(handler).not.toHaveBeenCalled(); expect(sent[0].messages[0].headers['dlq-reason']).toBe('missing-idempotency-key');
    });
});

describe('duplicate key detection', () => {
    it('recognises mongo and mysql errors', () => {
        expect(mongoIdempotencyStore({} as any).isDuplicateKeyError({ code: 11000 })).toBe(true);
        expect(mongoIdempotencyStore({} as any).isDuplicateKeyError({ code: 1 })).toBe(false);
        const my = mysqlIdempotencyStore({} as any);
        expect(my.isDuplicateKeyError({ name: 'SequelizeUniqueConstraintError' })).toBe(true);
        expect(my.isDuplicateKeyError({ original: { code: 'ER_DUP_ENTRY' } })).toBe(true);
        expect(my.isDuplicateKeyError(new Error('x'))).toBe(false);
    });
});
