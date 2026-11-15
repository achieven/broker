import { Consumer, ConsumerEvent } from './consumer';

/** Fake Confluent KafkaJS-compat client: records commits and DLQ transactions, lets a test deliver messages to eachMessage. */
function fake() {
    const commits: any[] = [];
    const dlq: any[] = [];
    const txOffsets: any[] = [];
    let each: any;
    const hooks = { failTx: false };
    const kafka = {
        producer: jest.fn((_config?: any) => ({
            connect: jest.fn(), disconnect: jest.fn(),
            send: jest.fn(async (r) => { dlq.push(r); }),
            transaction: jest.fn(async () => {
                const staged: any[] = [];
                return {
                    send: async (r: any) => { staged.push(r); },
                    sendOffsets: async (o: any) => { if (hooks.failTx) throw new Error('tx boom'); txOffsets.push(o); },
                    commit: async () => { dlq.push(...staged); },
                    abort: async () => {},
                };
            }),
        })),
        consumer: jest.fn((_config?: any) => ({
            connect: jest.fn(), disconnect: jest.fn(), subscribe: jest.fn(),
            commitOffsets: jest.fn(async (o) => { commits.push(...o); }),
            run: jest.fn(async (o) => { each = o.eachMessage; (kafka as any).runOptions = o; }),
        })),
    };
    const deliver = (value: string | null, offset = '7', extra: any = {}) =>
        each({ topic: 't', partition: 2, message: { key: Buffer.from('k'), value: value === null ? null : Buffer.from(value), offset, headers: {} }, heartbeat: async () => {}, ...extra });
    return { broker: { kafka } as any, kafka, commits, dlq, txOffsets, deliver, hooks };
}

const fast = { baseDelayMs: 1, maxDelayMs: 2 };
const started: Consumer[] = [];
afterEach(async () => { await Promise.all(started.splice(0).map((c) => c.stop())); }); // releases the transactional ids
const strict = (f: ReturnType<typeof fake>, handler: any) => {
    const events: ConsumerEvent[] = [];
    const consumer = new Consumer(f.broker).withGroupId('g').withTopic('t').withStrictProcessing().withMysqlClassifier()
        .withRetryPolicy(fast).onEvent((e) => events.push(e)).onRecord(handler);
    started.push(consumer);
    return { consumer, events };
};
const err = (errno: number) => Object.assign(new Error(`mysql ${errno}`), { errno });

describe('strict consumer (unit)', () => {
    it('runs with autoCommit off and commits offset+1 of that message after the handler', async () => {
        const f = fake();
        const handler = jest.fn(async () => { expect(f.commits).toHaveLength(0); }); // not before
        const { consumer } = strict(f, handler);
        await consumer.start(); await f.deliver('{"a":1}', '41');
        expect(f.kafka.consumer.mock.calls[0][0].kafkaJS.autoCommit).toBe(false);
        expect(f.commits).toEqual([{ topic: 't', partition: 2, offset: '42' }]);
    });

    it('permanent errors go to the DLQ at once, with the offset in the same transaction', async () => {
        const f = fake();
        const handler = jest.fn().mockRejectedValue(err(1406));
        const { consumer, events } = strict(f, handler);
        await consumer.start(); await f.deliver('{"a":1}', '9');
        expect(handler).toHaveBeenCalledTimes(1);
        expect(f.dlq[0].topic).toBe('t.dlq');
        expect(f.kafka.producer.mock.calls[0][0].kafkaJS).toMatchObject({ transactionalId: expect.any(String), acks: -1, idempotent: true });
        expect(f.dlq[0].messages[0].headers).toMatchObject({ 'x-error-class': 'permanent', 'x-source-offset': '9', 'x-source-partition': '2', 'x-stage': 'g' });
        expect(f.dlq[0].messages[0].key.toString()).toBe('k');
        expect(f.txOffsets[0]).toEqual({ consumer: f.kafka.consumer.mock.results[0].value, topics: [{ topic: 't', partitions: [{ partition: 2, offset: '10' }] }] });
        expect(f.commits).toHaveLength(0); // no separate commit
        expect(events.map((e) => e.type)).toEqual(['dlq']);
    });

    it('transient-message errors are retried maxAttempts times then dead-lettered as exhausted; unknown too', async () => {
        for (const e of [err(1213), new Error('???')]) {
            const f = fake();
            const handler = jest.fn().mockRejectedValue(e);
            const { consumer, events } = strict(f, handler);
            await consumer.withRetryPolicy({ ...fast, maxAttempts: 3 }).start(); await f.deliver('{}');
            expect(handler).toHaveBeenCalledTimes(3);
            expect(events.filter((x) => x.type === 'retry')).toHaveLength(2);
            expect(f.dlq[0].messages[0].headers['x-error-class']).toBe('exhausted');
            await consumer.stop(); // one transactional id per group+topic+instance
        }
    });

    it('transient-infra errors are retried until they stop, never dead-lettered, whatever maxAttempts says', async () => {
        const f = fake();
        let n = 0;
        const handler = jest.fn(async () => { if (++n < 12) throw err(2013); });
        const { consumer, events } = strict(f, handler);
        await consumer.withRetryPolicy({ ...fast, maxAttempts: 2 }).start(); await f.deliver('{}', '3');
        expect(handler).toHaveBeenCalledTimes(12);
        expect(f.dlq).toHaveLength(0);
        expect(events.filter((x) => x.type === 'retry')).toHaveLength(11);
        expect(f.commits).toEqual([{ topic: 't', partition: 2, offset: '4' }]);
    });

    it('stop() interrupts retries and leaves the offset uncommitted', async () => {
        const f = fake();
        const handler = jest.fn().mockRejectedValue(err(2013));
        const { consumer } = strict(f, handler);
        await consumer.withRetryPolicy({ baseDelayMs: 10_000, maxDelayMs: 10_000 }).start();
        const delivery = f.deliver('{}');
        await new Promise((r) => setTimeout(r, 50));
        await consumer.stop();
        await delivery; // returns promptly instead of sleeping 10 s
        expect(f.commits).toHaveLength(0);
        expect(f.dlq).toHaveLength(0);
    });

    it('markers override classification', async () => {
        const f = fake();
        const handler = jest.fn().mockRejectedValue(Object.assign(err(2013), { nonRetryable: true }));
        const { consumer } = strict(f, handler);
        await consumer.start(); await f.deliver('{}');
        expect(handler).toHaveBeenCalledTimes(1);
        expect(f.dlq[0].messages[0].headers['x-error-class']).toBe('permanent');
    });

    it('tombstones and empty values are dead-lettered without calling the handler or throwing', async () => {
        const f = fake();
        const handler = jest.fn();
        const { consumer } = strict(f, handler);
        await consumer.start();
        await f.deliver(null, '1'); await f.deliver('', '2');
        expect(handler).not.toHaveBeenCalled();
        expect(f.dlq.map((d) => d.messages[0].headers['x-error-class'])).toEqual(['tombstone', 'tombstone']);
        expect(f.dlq[0].messages[0].value).toBeNull();
    });

    it('duplicates: ack by default, dlq on request, and without a policy they are dead-lettered, never silently dropped', async () => {
        let f = fake();
        let s = strict(f, jest.fn().mockRejectedValue(err(1062)));
        await s.consumer.withIdempotency({}).start(); await f.deliver('{}', '5');
        expect(f.dlq).toHaveLength(0);
        expect(f.commits).toEqual([{ topic: 't', partition: 2, offset: '6' }]);
        expect(s.events[0]).toMatchObject({ type: 'processed', outcome: 'duplicate' });
        await s.consumer.stop();

        f = fake();
        s = strict(f, jest.fn().mockRejectedValue(err(1062)));
        await s.consumer.withIdempotency({ onDuplicate: 'dlq' }).start(); await f.deliver('{}');
        expect(f.dlq[0].messages[0].headers['x-error-class']).toBe('duplicate');
        await s.consumer.stop();

        f = fake();
        s = strict(f, jest.fn().mockRejectedValue(err(1062)));
        await s.consumer.start(); await f.deliver('{}');
        expect(f.dlq[0].messages[0].headers['x-error-class']).toBe('duplicate');
    });

    it('idempotency key: keyFrom, then header, then Kafka key; alreadyDone skips the handler; no key is fine unless required', async () => {
        const f = fake();
        const handler = jest.fn();
        const alreadyDone = jest.fn(async (k: string) => k === 'seen');
        const { consumer } = strict(f, handler);
        await consumer.withIdempotency({ keyFrom: (m) => m.json().id, alreadyDone }).start();
        await f.deliver('{"id":"seen"}'); expect(handler).not.toHaveBeenCalled();
        await f.deliver('{"id":"new"}'); expect(handler.mock.calls[0][0].idempotencyKey).toBe('new');
        await f.deliver('{}'); expect(handler.mock.calls[1][0].idempotencyKey).toBe('k'); // Kafka key fallback
        expect(f.commits).toHaveLength(3);
    });

    it('never calls ensureSchema through withIdempotency, only through the legacy store API', async () => {
        const ensureSchema = jest.fn();
        const store = { ensureSchema, exists: async () => false, isDuplicateKeyError: () => false };
        const f = fake();
        await strict(f, jest.fn()).consumer.withIdempotency({ alreadyDone: (k) => store.exists() }).start();
        expect(ensureSchema).not.toHaveBeenCalled();
        const g = fake();
        await new Consumer(g.broker).withGroupId('g').withTopic('t').withIdempotencyStore(store).onMessage(jest.fn()).start();
        expect(ensureSchema).toHaveBeenCalledTimes(1);
    });

    it('a DLQ transaction that fails is retried, never skipped, and the offset is not committed meanwhile', async () => {
        const f = fake();
        f.hooks.failTx = true;
        const { consumer, events } = strict(f, jest.fn().mockRejectedValue(err(1406)));
        await consumer.start();
        const delivery = f.deliver('{}');
        await new Promise((r) => setTimeout(r, 60));
        expect(f.dlq).toHaveLength(0); expect(f.txOffsets).toHaveLength(0); expect(f.commits).toHaveLength(0);
        expect(events.some((e) => e.type === 'retry' && e.phase === 'dlq')).toBe(true);
        f.hooks.failTx = false;
        await delivery;
        expect(f.dlq).toHaveLength(1);
        expect(f.txOffsets).toHaveLength(1);
    });

    it('the gate: halted blocks processing and heartbeats, a throwing gate is halted, clearing resumes, stop() exits', async () => {
        const f = fake();
        const handler = jest.fn();
        let mode: 'halt' | 'throw' | 'go' = 'halt';
        const heartbeat = jest.fn(async () => {});
        const { consumer } = strict(f, handler);
        await consumer.withGate(async () => { if (mode === 'throw') throw new Error('x'); return mode === 'halt'; }, { pollMs: 10 }).start();

        const delivery = f.deliver('{}', '1', { heartbeat });
        await new Promise((r) => setTimeout(r, 700));
        expect(handler).not.toHaveBeenCalled(); expect(f.commits).toHaveLength(0);
        expect(heartbeat).toHaveBeenCalled();
        mode = 'throw';
        await new Promise((r) => setTimeout(r, 100));
        expect(handler).not.toHaveBeenCalled();
        mode = 'go';
        await delivery;
        expect(handler).toHaveBeenCalledTimes(1);
        expect(f.commits).toHaveLength(1);

        mode = 'halt';
        await new Promise((r) => setTimeout(r, 50));
        const blocked = f.deliver('{}', '2', { heartbeat });
        await new Promise((r) => setTimeout(r, 50));
        await consumer.stop(); await blocked; // exits cleanly while halted
        expect(handler).toHaveBeenCalledTimes(1);
    });

    it('partition concurrency is passed to the client; commits are per message', async () => {
        const f = fake();
        const { consumer } = strict(f, jest.fn());
        await consumer.withPartitionConcurrency(8).start();
        expect((f.kafka as any).runOptions.partitionsConsumedConcurrently).toBe(8);
        expect(() => consumer.withPartitionConcurrency(0)).toThrow();
    });

    it('default mode is unchanged: auto-commit stays on, handler errors are rethrown for the client to retry', async () => {
        const f = fake();
        const consumer = new Consumer(f.broker).withGroupId('g').withTopic('t').onMessage(jest.fn().mockRejectedValue(new Error('boom')));
        await consumer.start();
        await expect(f.deliver('{"a":1}')).rejects.toThrow('boom');
        expect(f.kafka.consumer.mock.calls[0][0].kafkaJS.autoCommit).toBeUndefined();
        expect((f.kafka as any).runOptions.autoCommit).toBeUndefined();
        expect(f.commits).toHaveLength(0);
    });

    it('withRetryPolicy outside strict mode is rejected at start()', async () => {
        const f = fake();
        await expect(new Consumer(f.broker).withGroupId('g').withTopic('t').withRetryPolicy({}).onMessage(jest.fn()).start()).rejects.toThrow(/strict/i);
    });
});
