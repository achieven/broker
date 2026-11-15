import { Producer } from '../src/producer/producer';
import { IDEMPOTENCY_KEY_HEADER } from '../src/idempotency/idempotency-store';
import { ConfigResourceTypes } from 'kafkajs';
import { collect } from './helpers/collect';
import { durableTopic, newBroker, plainTopic, startProxies, uniq, waitFor } from './helpers/cluster';
import { ProxyGroup } from './helpers/proxy';

const payloads: Record<string, any> = {
    'flat object': { user_id: 'u1', amount: 12.5, ok: true },
    'nested object': { a: { b: { c: [1, 2, { d: 'deep' }] } } },
    'array': [1, 'two', null, { three: 3 }],
    'unicode and emoji': { text: 'héllo wörld – 你好 🎵🚀' },
    'null fields': { a: null, b: undefined, c: '' },
    'empty object': {},
    'string primitive': 'just a string',
    'number primitive': 42,
    'null': null,
    'large (500 KB)': { blob: 'x'.repeat(500 * 1024) },
};
const roundTripped = (v: any) => JSON.parse(JSON.stringify(v));

describe('Producer / Consumer round trip (real 3-broker cluster)', () => {
    let proxies: ProxyGroup;
    const broker = (() => { let b: ReturnType<typeof newBroker>; return () => (b ??= newBroker()); })();

    beforeAll(async () => { proxies = await startProxies(); });
    afterAll(async () => { await proxies.stop(); });

    describe.each([
        ['plain producer', async (name: string) => new Producer(broker()).withTopic(plainTopic(broker(), name))],
        ['durable producer', async (name: string) => new Producer(broker()).withDurability(durableTopic(broker(), name))],
    ])('%s', (_label, makeProducer) => {
        it.each(Object.keys(payloads))('delivers a %s intact', async (name) => {
            const topic = uniq('types');
            const producer = await (await makeProducer(topic)).connect();
            const { received, consumer } = await collect(broker(), topic);
            await producer.produce(payloads[name]);
            await waitFor(() => received.length === 1, 30000, 'message');
            expect(received[0].data).toEqual(roundTripped(payloads[name]));
            await consumer.stop(); await producer.disconnect();
        });

        it('tags every message with a unique idempotency key, and honours an explicit one', async () => {
            const topic = uniq('keys');
            const producer = await (await makeProducer(topic)).connect();
            const { received, consumer } = await collect(broker(), topic);
            const k1 = await producer.produce({ n: 1 });
            const k2 = await producer.produce({ n: 2 });
            const k3 = await producer.produce({ n: 3 }, { idempotencyKey: 'correlation-123' });
            await waitFor(() => received.length === 3, 30000, '3 messages');
            expect(k1).not.toEqual(k2);
            expect(k3).toBe('correlation-123');
            expect(received.map((r) => r.ctx.idempotencyKey).sort()).toEqual([k1, k2, 'correlation-123'].sort());
            await consumer.stop(); await producer.disconnect();
        });
    });

    describe('durable topics', () => {
        it('are created with replication factor 3, min.insync.replicas=2 and no unclean leader election', async () => {
            const topic = durableTopic(broker());
            await topic.ensure();
            const admin = broker().kafka.admin();
            await admin.connect();
            const { topics } = await admin.fetchTopicMetadata({ topics: [topic.name] });
            expect(topics[0].partitions.every((p) => p.replicas.length === 3)).toBe(true);
            const { resources } = await admin.describeConfigs({
                includeSynonyms: false,
                resources: [{ type: ConfigResourceTypes.TOPIC, name: topic.name }],
            });
            const cfg = Object.fromEntries(resources[0].configEntries.map((e) => [e.configName, e.configValue]));
            expect(cfg['min.insync.replicas']).toBe('2');
            expect(cfg['unclean.leader.election.enable']).toBe('false');
            await admin.disconnect();
        });

        it('refuse to be used when the topic already exists with weaker settings', async () => {
            const name = uniq('weak');
            const admin = broker().kafka.admin();
            await admin.connect();
            await admin.createTopics({ topics: [{ topic: name, numPartitions: 1, replicationFactor: 1 }], waitForLeaders: false });
            await admin.disconnect();

            await expect(durableTopic(broker(), name).ensure()).rejects.toThrow(/not durable.*replication factor is 1.*min\.insync\.replicas/);
            await expect(new Producer(broker()).withDurability(durableTopic(broker(), name)).connect()).rejects.toThrow(/not durable/);
        });
    });

    it('produce() before connect() fails loudly instead of dropping the event', async () => {
        const producer = new Producer(broker()).withTopic('whatever');
        await expect(producer.produce({ a: 1 })).rejects.toThrow(/not connected/);
    });
});
