import { collect } from './helpers/collect';
import { newBroker, plainTopic, sleep, startProxies, uniq, waitFor } from './helpers/cluster';
import { ProxyGroup } from './helpers/proxy';

describe('read_committed vs read_uncommitted consumers (transactional producer)', () => {
    let proxies: ProxyGroup;
    const broker = newBroker();

    beforeAll(async () => { proxies = await startProxies(); });
    afterAll(async () => { await proxies.stop(); });

    const txProducer = async () => {
        const p = broker.kafka.producer({ kafkaJS: { transactionalId: uniq('tx'), idempotent: true, maxInFlightRequests: 1 } });
        await p.connect();
        return p;
    };
    const msg = (id: string) => ({ value: JSON.stringify({ id }) });
    // de-duplicated: after a consumer-group rebalance (slow CI box) Kafka may legitimately redeliver a record
    const ids = (r: Array<{ data: any }>) => [...new Set(r.map((x) => x.data.id))];

    it('read_committed hides records of an open transaction until it commits; read_uncommitted sees them immediately', async () => {
        const topic = plainTopic(broker); await topic.ensure();
        const uncommitted = await collect(broker, topic.name);
        const committed = await collect(broker, topic.name, (c) => c.withReadCommitted());

        const producer = await txProducer();
        const tx = await producer.transaction();
        await tx.send({ topic: topic.name, messages: [msg('in-open-tx')] });

        await waitFor(() => uncommitted.received.length === 1, 30000, 'uncommitted consumer to see the open-transaction record');
        await sleep(5000);
        expect(ids(committed.received)).toEqual([]); // still hidden

        await tx.commit();
        await waitFor(() => committed.received.length === 1, 30000, 'committed consumer to see the record after commit');
        expect(ids(committed.received)).toEqual(['in-open-tx']);
        expect(ids(uncommitted.received)).toEqual(['in-open-tx']);

        await Promise.all([uncommitted.consumer.stop(), committed.consumer.stop(), producer.disconnect()]);
    });

    it('read_committed never sees records of an aborted transaction; read_uncommitted does', async () => {
        const topic = plainTopic(broker); await topic.ensure();
        const uncommitted = await collect(broker, topic.name);
        const committed = await collect(broker, topic.name, (c) => c.withReadCommitted());

        const producer = await txProducer();
        const aborted = await producer.transaction();
        await aborted.send({ topic: topic.name, messages: [msg('aborted')] });
        await aborted.abort();
        const ok = await producer.transaction();
        await ok.send({ topic: topic.name, messages: [msg('committed')] });
        await ok.commit();

        await waitFor(() => committed.received.length >= 1, 30000, 'committed record');
        await waitFor(() => uncommitted.received.length >= 2, 30000, 'both records on the uncommitted consumer');
        await sleep(2000);
        expect(ids(committed.received)).toEqual(['committed']);
        expect(ids(uncommitted.received).sort()).toEqual(['aborted', 'committed']);

        await Promise.all([uncommitted.consumer.stop(), committed.consumer.stop(), producer.disconnect()]);
    });

    it('consumers are read_uncommitted by default', async () => {
        const topic = plainTopic(broker); await topic.ensure();
        const producer = await txProducer();
        const tx = await producer.transaction();
        await tx.send({ topic: topic.name, messages: [msg('open')] });
        const byDefault = await collect(broker, topic.name);
        await waitFor(() => byDefault.received.length === 1, 30000, 'default consumer to see the open-transaction record');
        await tx.abort();
        await Promise.all([byDefault.consumer.stop(), producer.disconnect()]);
    });
});
