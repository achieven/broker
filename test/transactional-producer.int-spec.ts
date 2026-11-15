import { Producer } from '../src/producer/producer';
import { collect } from './helpers/collect';
import { durableTopic, newBroker, plainTopic, sleep, startProxies, uniq, waitFor } from './helpers/cluster';
import { ProxyGroup } from './helpers/proxy';

describe('transactional producer', () => {
    let proxies: ProxyGroup;
    const broker = newBroker();
    const ids = (r: Array<{ data: any }>) => r.map((x) => x.data.id).sort();

    beforeAll(async () => {
        proxies = await startProxies();
        process.env.PRODUCER_INSTANCE_ID = 'it-instance-1';
    });
    afterAll(async () => { delete process.env.PRODUCER_INSTANCE_ID; await proxies.stop(); });

    it('generates a deterministic transactional id: same logical producer -> same id after a restart', async () => {
        const topic = plainTopic(broker);
        const first = await new Producer(broker).withTopic(topic).withTransactions().connect();
        const id = first.transactionalId;
        expect(id).toBe(`tx-${topic.name}-it-instance-1`);
        await first.disconnect();

        const restarted = await new Producer(broker).withTopic(topic).withTransactions().connect();
        expect(restarted.transactionalId).toBe(id);
        await restarted.disconnect();
    });

    it('refuses two live producers with the same id in one process (they would fence each other)', async () => {
        const topic = plainTopic(broker);
        const a = await new Producer(broker).withTopic(topic).withTransactions().connect();
        await expect(new Producer(broker).withTopic(topic).withTransactions().connect()).rejects.toThrow(/already used/);
        const b = await new Producer(broker).withTopic(topic).withTransactions({ transactionalId: uniq('explicit') }).connect();
        await a.disconnect();
        await new Producer(broker).withTopic(topic).withTransactions().connect().then((p) => p.disconnect()); // id released
        await b.disconnect();
    });

    it('produce() commits each message in its own transaction, tagged with an idempotency key', async () => {
        const topic = plainTopic(broker);
        const producer = await new Producer(broker).withTopic(topic).withTransactions().connect();
        const { received, consumer } = await collect(broker, topic.name, (c) => c.withReadCommitted());
        const key = await producer.produce({ id: 'single' });
        await waitFor(() => received.length === 1, 30000, 'committed message');
        expect(received[0].ctx.idempotencyKey).toBe(key);
        await Promise.all([consumer.stop(), producer.disconnect()]);
    });

    it('transaction(): a group of messages across two topics becomes visible all at once, only on commit', async () => {
        const a = plainTopic(broker); const b = plainTopic(broker);
        await Promise.all([a.ensure(), b.ensure()]);
        const producer = await new Producer(broker).withTopic(a).withTransactions().connect();
        const onA = await collect(broker, a.name, (c) => c.withReadCommitted());
        const onB = await collect(broker, b.name, (c) => c.withReadCommitted());

        let release!: () => void;
        const hold = new Promise<void>((r) => (release = r));
        const done = producer.transaction(async (tx) => {
            await tx.produce({ id: 'a1' });
            await tx.produce({ id: 'a2' });
            await tx.produce({ id: 'b1' }, { topic: b });
            await hold; // transaction still open
        });

        await sleep(4000);
        expect([onA.received.length, onB.received.length]).toEqual([0, 0]); // nothing visible yet
        release();
        await done;
        await waitFor(() => onA.received.length === 2 && onB.received.length === 1, 30000, 'committed group');
        expect(ids(onA.received)).toEqual(['a1', 'a2']);
        expect(ids(onB.received)).toEqual(['b1']);
        await Promise.all([onA.consumer.stop(), onB.consumer.stop(), producer.disconnect()]);
    });

    it('transaction(): if the callback throws, everything is aborted (invisible to read_committed), the error is rethrown, and the producer keeps working', async () => {
        const topic = plainTopic(broker); await topic.ensure();
        const producer = await new Producer(broker).withTopic(topic).withTransactions().connect();
        const committed = await collect(broker, topic.name, (c) => c.withReadCommitted());
        const everything = await collect(broker, topic.name);

        await expect(producer.transaction(async (tx) => {
            await tx.produce({ id: 'doomed-1' });
            await tx.produce({ id: 'doomed-2' });
            throw new Error('business failure');
        })).rejects.toThrow('business failure');
        await producer.produce({ id: 'after' });

        await waitFor(() => committed.received.length === 1 && everything.received.length === 3, 30000, 'records');
        expect(ids(committed.received)).toEqual(['after']);
        expect(ids(everything.received)).toEqual(['after', 'doomed-1', 'doomed-2']); // aborted records exist, but are hidden
        await Promise.all([committed.consumer.stop(), everything.consumer.stop(), producer.disconnect()]);
    });

    it('concurrent transaction() calls on one producer are serialised, not interleaved or failed', async () => {
        const topic = plainTopic(broker); await topic.ensure();
        const producer = await new Producer(broker).withTopic(topic).withTransactions().connect();
        const { received, consumer } = await collect(broker, topic.name, (c) => c.withReadCommitted());
        await Promise.all([1, 2, 3, 4].map((n) => producer.transaction(async (tx) => {
            await tx.produce({ id: `t${n}-a` }); await tx.produce({ id: `t${n}-b` });
        })));
        await waitFor(() => received.length === 8, 30000, '8 records');
        // each transaction's two records are adjacent in the log
        const order = received.map((r) => r.data.id);
        for (let n = 1; n <= 4; n++) expect(order.indexOf(`t${n}-b`) - order.indexOf(`t${n}-a`)).toBe(1);
        await Promise.all([consumer.stop(), producer.disconnect()]);
    });

    it('a new producer with the same id fences the old one and aborts its half-finished transaction', async () => {
        const topic = plainTopic(broker); await topic.ensure();
        const id = uniq('crashy');
        const committed = await collect(broker, topic.name, (c) => c.withReadCommitted());

        // "crashed" producer: opens a transaction, writes, never commits
        const zombie = broker.kafka.producer({ kafkaJS: { transactionalId: id, idempotent: true, maxInFlightRequests: 1 } });
        await zombie.connect();
        const open = await zombie.transaction();
        await open.send({ topic: topic.name, messages: [{ value: JSON.stringify({ id: 'zombie' }) }] });

        // its replacement starts with the same id (what a restart of the same logical producer does)
        const replacement = await new Producer(broker).withTopic(topic).withTransactions({ transactionalId: id }).connect();
        await replacement.produce({ id: 'replacement' });

        await waitFor(() => committed.received.length === 1, 30000, 'replacement record');
        expect(ids(committed.received)).toEqual(['replacement']); // zombie's transaction was aborted, not left blocking
        await expect(open.send({ topic: topic.name, messages: [{ value: JSON.stringify({ id: 'late' }) }] })).rejects.toThrow(); // fenced
        await Promise.all([committed.consumer.stop(), replacement.disconnect(), zombie.disconnect().catch(() => {})]);
    });

    it('composes with durability: withDurability(topic).withTransactions()', async () => {
        const topic = durableTopic(broker);
        const producer = await new Producer(broker).withDurability(topic).withTransactions().connect();
        const { received, consumer } = await collect(broker, topic.name, (c) => c.withReadCommitted());
        await producer.transaction(async (tx) => { await tx.produce({ id: 'd1' }); await tx.produce({ id: 'd2' }); });
        await waitFor(() => received.length === 2, 30000, 'durable transactional messages');
        expect(ids(received)).toEqual(['d1', 'd2']);
        await Promise.all([consumer.stop(), producer.disconnect()]);
    });

    it('transaction() on a non-transactional producer fails loudly', async () => {
        const producer = await new Producer(broker).withTopic(plainTopic(broker)).connect();
        await expect(producer.transaction(async () => {})).rejects.toThrow(/not transactional/);
        await producer.disconnect();
    });
});
