import { Producer } from '../src/producer/producer';
import { docker, durableTopic, newBroker, readRaw, startProxies, waitFor } from './helpers/cluster';
import { ProxyGroup } from './helpers/proxy';

describe('durable producer when a broker dies', () => {
    let proxies: ProxyGroup;
    const broker = newBroker();

    beforeAll(async () => { proxies = await startProxies(); });
    afterAll(async () => {
        docker('start broker-test-kafka3'); // leave the cluster whole for whoever runs next
        await waitFor(() => docker('inspect -f {{.State.Health.Status}} broker-test-kafka3').trim() === 'healthy', 120000, 'kafka3 healthy');
        await proxies.stop();
    });

    it('still acknowledges writes with one of three brokers down (min.insync.replicas=2) and loses nothing', async () => {
        const topic = durableTopic(broker);
        const producer = await new Producer(broker).withDurability(topic).connect();
        await producer.produce({ n: 1 });

        docker('stop broker-test-kafka3'); // bootstrap is broker 1, so the client stays connected
        // The dead broker stays in the ISR until replica.lag.time.max.ms (30s), so this send is retried until it shrinks.
        await producer.produce({ n: 2 });
        await producer.produce({ n: 3 });

        const records = await readRaw(broker.kafka, topic.name);
        expect(records.map((r) => JSON.parse(r.value!).n).sort()).toEqual([1, 2, 3]);
        await producer.disconnect();
    });
});
