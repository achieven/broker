import { Producer } from '../src/producer/producer';
import { countRecords, durableTopic, newBroker, plainTopic, readRaw, startProxies } from './helpers/cluster';
import { ProxyGroup } from './helpers/proxy';

/**
 * Fault: the broker appends the batch, but the client never gets the response and its connection is closed,
 * so the client retries the same batch. An idempotent producer must not duplicate it.
 */
describe('producer retries after a lost acknowledgement', () => {
    let proxies: ProxyGroup;
    const broker = newBroker();

    beforeAll(async () => { proxies = await startProxies(); });
    afterAll(async () => { await proxies.stop(); });

    it('durable (idempotent, acks=all) producer writes the retried message exactly once', async () => {
        const topic = durableTopic(broker);
        const producer = await new Producer(broker).withDurability(topic).connect();
        await producer.produce({ n: 1 }); // warm up connections / metadata

        proxies.armDropNextProduceResponse();
        await producer.produce({ n: 2 }); // resolves only after the client retried
        expect(proxies.drops).toBe(1); // the fault really happened

        expect(await countRecords(broker.kafka, topic.name)).toBe(2);
        const records = await readRaw(broker.kafka, topic.name);
        expect(records.map((r) => JSON.parse(r.value!).n).sort()).toEqual([1, 2]);
        await producer.disconnect();
    });

    it('withIdempotence() makes a producer on a plain topic write the retried message exactly once', async () => {
        const topic = plainTopic(broker);
        const producer = await new Producer(broker).withTopic(topic).withIdempotence().connect();
        await producer.produce({ n: 1 });

        proxies.armDropNextProduceResponse();
        await producer.produce({ n: 2 });
        expect(proxies.drops).toBe(1);

        expect(await countRecords(broker.kafka, topic.name)).toBe(2);
        await producer.disconnect();
    });

    it('control: a non-idempotent producer DOES duplicate under the same fault (proves the fault injection works)', async () => {
        const topic = plainTopic(broker);
        const producer = await new Producer(broker).withTopic(topic).connect();
        await producer.produce({ n: 1 });

        proxies.armDropNextProduceResponse();
        await producer.produce({ n: 2 });
        expect(proxies.drops).toBe(1);

        expect(await countRecords(broker.kafka, topic.name)).toBe(3); // 1 + the retried message twice
        await producer.disconnect();
    });

    it('the retried message keeps the same idempotency key, so downstream dedupe also holds', async () => {
        const topic = plainTopic(broker);
        const producer = await new Producer(broker).withTopic(topic).connect();
        await producer.produce({ n: 1 });
        proxies.armDropNextProduceResponse();
        const key = await producer.produce({ n: 2 });

        const records = await readRaw(broker.kafka, topic.name);
        const dupes = records.filter((r) => JSON.parse(r.value!).n === 2);
        expect(dupes).toHaveLength(2); // non-idempotent producer duplicated it...
        expect(new Set(dupes.map((d) => d.headers['idempotency-key']))).toEqual(new Set([key])); // ...with an identical key
        await producer.disconnect();
    });
});
