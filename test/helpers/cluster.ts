import { execSync } from 'child_process';
import { randomUUID } from 'crypto';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { BrokerService } from '../../src/broker.service';
import { TopicService } from '../../src/topic/topic.service';
import { DurableTopicService } from '../../src/topic/durable-topic.service';
import { ProxyGroup } from './proxy';

export const PROXY_BOOTSTRAP_PORT = 49092;

/** brokers 1..3: clients connect to 4909x (proxy) -> 3909x (docker published port) -> broker */
export function startProxies() {
    const group = new ProxyGroup([[49092, 39092], [49093, 39093], [49094, 39094]]);
    return group.start().then(() => group);
}

const clients: Array<{ disconnect(): Promise<unknown> }> = [];

export function newBroker(): BrokerService {
    process.env.BROKER_HOST = 'localhost';
    process.env.BROKER_PORT = String(PROXY_BOOTSTRAP_PORT);
    const broker = new BrokerService();
    // Remember every producer/consumer/admin created through this broker (including the ones the library creates
    // internally) so the harness can close whatever a test left behind. See disconnectAllClients().
    const kafka: any = broker.kafka;
    for (const factory of ['producer', 'consumer', 'admin']) {
        const original = kafka[factory].bind(kafka);
        kafka[factory] = (...args: any[]) => {
            const client = original(...args);
            clients.push(client);
            return client;
        };
    }
    return broker;
}

/**
 * A Kafka client that outlives its test file keeps running inside the shared Jest process, and its next lazy
 * `require` makes Jest print "You are trying to `require` a file after the Jest environment has been torn down" and
 * set exit code 1, even though every test passed. So every file ends by disconnecting everything it created.
 */
export async function disconnectAllClients() {
    await Promise.all(clients.splice(0).map((c) =>
        Promise.race([c.disconnect().catch(() => {}), new Promise((r) => setTimeout(r, 10000).unref())]),
    ));
}

export const uniq = (prefix: string) => `${prefix}-${randomUUID().slice(0, 8)}`;

export class PlainTopic extends TopicService {}
export class StrongTopic extends DurableTopicService {}

export const plainTopic = (broker: BrokerService, name = uniq('plain'), numPartitions = 1) =>
    new PlainTopic(broker, name, { numPartitions });
export const durableTopic = (broker: BrokerService, name = uniq('durable'), numPartitions = 1) =>
    new StrongTopic(broker, name, { numPartitions });

export async function waitFor<T>(fn: () => T | Promise<T>, timeoutMs = 60000, what = 'condition'): Promise<NonNullable<T>> {
    const start = Date.now();
    for (;;) {
        const v = await fn();
        if (v) return v as NonNullable<T>;
        if (Date.now() - start > timeoutMs) throw new Error(`Timed out waiting for ${what}`);
        await new Promise((r) => setTimeout(r, 200));
    }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Number of records physically in the topic (high - low watermark, all partitions). Non-transactional topics only. */
export async function countRecords(kafka: KafkaJS.Kafka, topic: string): Promise<number> {
    const admin = kafka.admin();
    await admin.connect();
    try {
        const offsets = await admin.fetchTopicOffsets(topic);
        return offsets.reduce((n, p) => n + (Number(p.high) - Number(p.low)), 0);
    } finally {
        await admin.disconnect();
    }
}

/** Reads a topic from the beginning with a raw consumer (no idempotency logic) until `count` records arrived or idle. */
export async function readRaw(kafka: KafkaJS.Kafka, topic: string, opts: { readUncommitted?: boolean; idleMs?: number } = {}) {
    const consumer = kafka.consumer({ kafkaJS: { groupId: uniq('raw'), readUncommitted: opts.readUncommitted ?? true, fromBeginning: true } });
    const got: Array<{ key: string | undefined; value: string | undefined; headers: Record<string, string> }> = [];
    let last = Date.now();
    await consumer.connect();
    await consumer.subscribe({ topic });
    await consumer.run({
        eachMessage: async ({ message }) => {
            last = Date.now();
            const headers: Record<string, string> = {};
            for (const [k, v] of Object.entries(message.headers ?? {})) headers[k] = String(v);
            got.push({ key: message.key?.toString(), value: message.value?.toString(), headers });
        },
    });
    await sleep(2000); // join the group
    while (Date.now() - last < (opts.idleMs ?? 3000)) await sleep(250);
    await consumer.disconnect();
    return got;
}

export const docker = (cmd: string) => execSync(`docker ${cmd}`, { stdio: 'pipe' }).toString();

/** Waits until the consumer group has a member with partitions assigned, i.e. it will actually receive new messages. */
export async function waitForGroupStable(kafka: KafkaJS.Kafka, groupId: string, timeoutMs = 90000) {
    const admin = kafka.admin();
    await admin.connect();
    try {
        await waitFor(async () => {
            const { groups } = await admin.describeGroups([groupId]);
            const g = groups[0];
            return g?.state === KafkaJS.ConsumerGroupStates.STABLE && g.members.length > 0;
        }, timeoutMs, `consumer group ${groupId} to become stable`);
    } finally {
        await admin.disconnect();
    }
}

/** The offset the group has committed for a partition: the NEXT offset to read, or -1 if nothing was ever committed. */
export async function committedOffset(kafka: KafkaJS.Kafka, groupId: string, topic: string, partition = 0): Promise<number> {
    const admin = kafka.admin();
    await admin.connect();
    try {
        const [t] = await admin.fetchOffsets({ groupId, topics: [topic] });
        const offset = Number(t.partitions.find((p) => p.partition === partition)?.offset ?? -1);
        return offset < 0 ? -1 : offset; // librdkafka reports "no committed offset" as a negative sentinel
    } finally {
        await admin.disconnect();
    }
}
