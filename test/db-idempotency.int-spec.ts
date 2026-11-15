import { MongoClient } from 'mongodb';
import { DataTypes, Model, ModelStatic, Sequelize } from 'sequelize';
import { Producer } from '../src/producer/producer';
import { Consumer } from '../src/consumer/consumer';
import { IdempotencyStore } from '../src/idempotency/idempotency-store';
import { mongoIdempotencyStore } from '../src/idempotency/mongo-idempotency-store';
import { mysqlIdempotencyStore } from '../src/idempotency/mysql-idempotency-store';
import { newBroker, plainTopic, readRaw, sleep, startProxies, uniq, waitFor, waitForGroupStable } from './helpers/cluster';
import { ProxyGroup } from './helpers/proxy';

/** One adapter per database; the same scenarios run against both. */
interface Backend {
    name: string;
    /** Fresh, empty target; `withCheck` wires the consumer to it. */
    create(): Promise<{
        withCheck(c: Consumer): Consumer;
        store: IdempotencyStore;
        insert(key: string, data: any): Promise<void>;
        count(): Promise<number>;
    }>;
    close(): Promise<void>;
}

const mongoBackend = (): Backend => {
    const client = new MongoClient('mongodb://localhost:37017');
    return {
        name: 'MongoDB',
        async create() {
            await client.connect();
            const collection = client.db('it').collection(uniq('docs'));
            return {
                withCheck: (c) => c.withMongoIdempotencyCheck(collection),
                store: mongoIdempotencyStore(collection),
                insert: async (idempotencyKey, data) => { await collection.insertOne({ idempotencyKey, data }); },
                count: () => collection.countDocuments(),
            };
        },
        close: () => client.close(),
    };
};

const mysqlBackend = (): Backend => {
    const sequelize = new Sequelize('test', 'root', 'root', { host: 'localhost', port: 33306, dialect: 'mysql', logging: false });
    return {
        name: 'MySQL',
        async create() {
            await sequelize.authenticate();
            // NOTE: no `unique` on the attribute - the consumer's ensureSchema() must add the constraint itself.
            const Doc: ModelStatic<Model> = sequelize.define(uniq('docs').replace(/-/g, '_'), {
                idempotencyKey: { type: DataTypes.STRING(64), allowNull: false },
                payload: { type: DataTypes.TEXT },
            }, { timestamps: false });
            await Doc.sync();
            return {
                withCheck: (c) => c.withMysqlIdempotencyCheck(Doc),
                store: mysqlIdempotencyStore(Doc),
                insert: async (idempotencyKey, data) => { await Doc.create({ idempotencyKey, payload: JSON.stringify(data) }); },
                count: () => Doc.count(),
            };
        },
        close: () => sequelize.close(),
    };
};

describe.each([mongoBackend(), mysqlBackend()])('idempotency between the broker and $name', (backend) => {
    let proxies: ProxyGroup;
    const broker = newBroker();

    beforeAll(async () => { proxies = await startProxies(); });
    afterAll(async () => { await proxies.stop(); await backend.close(); });

    /** A consumer whose handler does the real work: insert the document carrying the idempotency key. */
    const worker = async (db: Awaited<ReturnType<Backend['create']>>, topic: string, groupId = uniq('g'), beforeInsert?: () => Promise<void>) => {
        const calls: string[] = [];
        const consumer = db.withCheck(new Consumer(broker).withGroupId(groupId).withTopic(topic)).onMessage(async (data, ctx) => {
            calls.push(ctx.idempotencyKey!);
            await beforeInsert?.();
            await db.insert(ctx.idempotencyKey!, data);
        });
        await consumer.start();
        return { calls, consumer, groupId };
    };

    it('the consumer creates a UNIQUE constraint on the key, enforced by the database itself', async () => {
        const db = await backend.create();
        const topic = plainTopic(broker); await topic.ensure();
        const { consumer } = await worker(db, topic.name);

        await db.insert('same-key', { n: 1 });
        const second = await db.insert('same-key', { n: 2 }).then(() => null, (e) => e);
        expect(second).not.toBeNull();
        expect(db.store.isDuplicateKeyError(second)).toBe(true);
        expect(await db.count()).toBe(1);
        await consumer.stop();
    });

    it('processes a new event once and skips a redelivery carrying the same idempotency key', async () => {
        const db = await backend.create();
        const topic = plainTopic(broker); await topic.ensure();
        const producer = await new Producer(broker).withTopic(topic).connect();
        const { calls, consumer } = await worker(db, topic.name);

        const key = await producer.produce({ n: 1 });
        await waitFor(async () => (await db.count()) === 1, 30000, 'first document');

        await producer.produce({ n: 1 }, { idempotencyKey: key }); // the same logical event arrives again
        await producer.produce({ n: 2 }); // and a different one right after, to know the duplicate was already passed
        await waitFor(async () => (await db.count()) === 2, 30000, 'second document');
        await sleep(1000);

        expect(calls).toHaveLength(2); // handler ran for 2 events, not 3 deliveries
        expect(await db.count()).toBe(2);
        expect(await readRaw(broker.kafka, `${topic.name}.dlq`).catch(() => [])).toHaveLength(0); // skipped, not dead-lettered
        await Promise.all([consumer.stop(), producer.disconnect()]);
    });

    it('concurrent duplicates: the unique constraint lets exactly one win and the loser goes to the DLQ without retries', async () => {
        const db = await backend.create();
        const topic = plainTopic(broker); await topic.ensure();
        const producer = await new Producer(broker).withTopic(topic).connect();

        // Two independent consumers (different groups) get the same event at the same time. A barrier holds both
        // between the idempotency pre-read (both saw "not there yet") and the insert, so the race is deterministic.
        let entered = 0;
        let release!: () => void;
        const bothEntered = new Promise<void>((r) => (release = r));
        const barrier = async () => {
            if (++entered === 2) release();
            let timer: NodeJS.Timeout;
            const fallback = new Promise<void>((r) => (timer = setTimeout(r, 60000)));
            await Promise.race([bothEntered, fallback]);
            clearTimeout(timer!); // a leftover 60s timer would keep the Jest process alive after the run
        };

        const a = await worker(db, topic.name, uniq('A'), barrier);
        const b = await worker(db, topic.name, uniq('B'), barrier);
        // start() returns before the group join finishes; on a slow runner the second consumer could still be joining
        // when the event arrives, and then it would see the document already written and skip instead of racing.
        await Promise.all([waitForGroupStable(broker.kafka, a.groupId), waitForGroupStable(broker.kafka, b.groupId)]);
        const key = await producer.produce({ n: 1 });

        const dlq = plainTopic(broker, `${topic.name}.dlq`);
        await waitFor(async () => (await db.count()) === 1 && entered === 2, 60000, 'one winner');
        const dead = await waitFor(async () => { const r = await readRaw(broker.kafka, dlq.name, { idleMs: 1500 }).catch(() => []); return r.length ? r : null; }, 60000, 'DLQ entry');
        await sleep(3000); // would reveal retries

        expect(await db.count()).toBe(1);
        expect(entered).toBe(2); // each handler ran exactly once: fail fast, no retry
        expect(a.calls.length + b.calls.length).toBe(2);
        expect(dead).toHaveLength(1);
        expect(dead[0].headers['dlq-reason']).toBe('duplicate-idempotency-key');
        expect(dead[0].headers['idempotency-key']).toBe(key);
        expect(dead[0].headers['dlq-source-topic']).toBe(topic.name);
        expect(JSON.parse(dead[0].value!)).toEqual({ n: 1 }); // original payload preserved for inspection / replay
        await Promise.all([a.consumer.stop(), b.consumer.stop(), producer.disconnect()]);
    });

    it('a message with no idempotency key is dead-lettered immediately and never reaches the handler', async () => {
        const db = await backend.create();
        const topic = plainTopic(broker); await topic.ensure();
        const { calls, consumer } = await worker(db, topic.name);

        const raw = broker.kafka.producer();
        await raw.connect();
        await raw.send({ topic: topic.name, messages: [{ value: JSON.stringify({ n: 1 }) }] }); // no header
        await raw.disconnect();

        const dead = await waitFor(async () => { const r = await readRaw(broker.kafka, `${topic.name}.dlq`, { idleMs: 1500 }).catch(() => []); return r.length ? r : null; }, 60000, 'DLQ entry');
        expect(dead[0].headers['dlq-reason']).toBe('missing-idempotency-key');
        expect(calls).toHaveLength(0);
        expect(await db.count()).toBe(0);
        await consumer.stop();
    });
});
