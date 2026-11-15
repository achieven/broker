import { KafkaJS } from '@confluentinc/kafka-javascript';
import { DataTypes, Model, ModelStatic, Sequelize } from 'sequelize';
import { Consumer, ConsumerEvent, ConsumedMessage } from '../src/consumer/consumer';
import { committedOffset, newBroker, plainTopic, readRaw, sleep, startProxies, uniq, waitFor } from './helpers/cluster';
import { track } from './helpers/collect';
import { ProxyGroup, TcpProxy } from './helpers/proxy';

/**
 * Acceptance tests for the strict (payments) consumer, against a real 3-broker Kafka and a real MySQL.
 * The handler does what a service does: write the message into a MySQL table (schema owned by the test, the consumer
 * never touches it), so permanent / duplicate / outage failures are real MySQL errors.
 */
const MYSQL_PORT = 33306;
const MYSQL_PROXY_PORT = 43306;
const FAST_RETRY = { baseDelayMs: 20, maxDelayMs: 200 };

describe('strict consumer', () => {
    const broker = newBroker();
    let proxies: ProxyGroup;
    let mysqlProxy: TcpProxy;
    let sequelize: Sequelize;
    let Row: ModelStatic<Model>;

    beforeAll(async () => {
        proxies = await startProxies();
        mysqlProxy = new TcpProxy(MYSQL_PROXY_PORT, MYSQL_PORT);
        await mysqlProxy.start();
        sequelize = new Sequelize('test', 'root', 'root', {
            host: '127.0.0.1', port: MYSQL_PROXY_PORT, dialect: 'mysql', logging: false,
            pool: { max: 5, acquire: 5000 },
        });
        await sequelize.authenticate();
        Row = sequelize.define(uniq('strict').replace(/-/g, '_'), {
            msgId: { type: DataTypes.STRING(64), allowNull: false, unique: true }, // the unique constraint we own
            note: { type: DataTypes.STRING(8) }, // short on purpose: a longer value is a real ER_DATA_TOO_LONG (1406)
        }, { timestamps: false });
        await Row.sync();
    });
    afterAll(async () => {
        await sequelize.close();
        await mysqlProxy.stop();
        await proxies.stop();
    });

    const insert = async (m: ConsumedMessage) => {
        const { msgId, note } = m.json();
        await Row.create({ msgId, note: note ?? 'ok' });
    };
    const rowIds = async () => (await Row.findAll({ raw: true }) as any[]).map((r) => r.msgId as string);

    /** Topic (+ its DLQ, so readers can subscribe before anything is written to it). */
    async function topics(partitions = 1) {
        const source = plainTopic(broker, uniq('src'), partitions);
        const dlq = plainTopic(broker, `${source.name}.dlq`);
        await source.ensure();
        await dlq.ensure();
        return { topic: source.name, dlq: dlq.name };
    }

    async function send(topic: string, messages: Array<{ key?: string; value: string | null; partition?: number; headers?: Record<string, string> }>) {
        const producer = broker.kafka.producer({ kafkaJS: { idempotent: true, acks: -1, maxInFlightRequests: 1 } });
        await producer.connect();
        await producer.send({ topic, messages });
        await producer.disconnect();
    }
    const json = (o: object) => JSON.stringify(o);
    /** The client objects are classes with private state, so they can't be spread: wrap them, binding methods to the real object. */
    const overriding = <T extends object>(target: T, overrides: Record<string, unknown>): T =>
        new Proxy(target, {
            get: (t: any, prop) => (prop in overrides ? overrides[prop as string] : typeof t[prop] === 'function' ? t[prop].bind(t) : t[prop]),
        });

    /** Strict consumer with MySQL classification, recording every event. */
    function strictConsumer(topic: string, groupId = uniq('g')) {
        const events: ConsumerEvent[] = [];
        const consumer = track(new Consumer(broker)).withGroupId(groupId).withTopic(topic)
            .withStrictProcessing().withMysqlClassifier().withReadCommitted()
            .withStage('executor').withRetryPolicy(FAST_RETRY)
            .onEvent((e) => events.push(e));
        return { consumer, events, groupId };
    }
    const of = <T extends ConsumerEvent['type']>(events: ConsumerEvent[], type: T) =>
        events.filter((e): e is Extract<ConsumerEvent, { type: T }> => e.type === type);

    // ------------------------------------------------------------------------------------------------ errors

    it('a poison message is dead-lettered immediately and the next message is processed', async () => {
        const { topic, dlq } = await topics();
        const { consumer, events, groupId } = strictConsumer(topic);
        const calls: string[] = [];
        await consumer.onRecord(async (m) => { calls.push(m.json().msgId); await insert(m); }).start();

        const bad = uniq('poison'), good = uniq('good');
        await send(topic, [
            { key: 'k1', value: json({ msgId: bad, note: 'far-too-long-for-the-column' }), headers: { trace: 'abc' } },
            { key: 'k2', value: json({ msgId: good }) },
        ]);

        await waitFor(async () => (await rowIds()).includes(good), 60000, 'the good message to be processed');
        expect(calls.filter((c) => c === bad)).toHaveLength(1); // no retries for a permanent error
        expect(of(events, 'retry')).toHaveLength(0);
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 2, 30000, 'both offsets to be committed');

        const dead = await readRaw(broker.kafka, dlq, { readUncommitted: false });
        expect(dead).toHaveLength(1);
        expect(dead[0].key).toBe('k1');
        expect(JSON.parse(dead[0].value!).msgId).toBe(bad); // original value preserved
        expect(dead[0].headers).toMatchObject({
            trace: 'abc', // original headers preserved
            'x-stage': 'executor', 'x-error-class': 'permanent',
            'x-source-topic': topic, 'x-source-partition': '0', 'x-source-offset': '0',
        });
        expect(dead[0].headers['x-error']).toMatch(/too long/i);
        expect(of(events, 'dlq')).toHaveLength(1);
    });

    it('a deadlock-style error is retried maxAttempts times, then dead-lettered as exhausted', async () => {
        const { topic, dlq } = await topics();
        const { consumer, events, groupId } = strictConsumer(topic);
        const deadlock = () => Object.assign(new Error('Deadlock found when trying to get lock'), {
            name: 'SequelizeDatabaseError', parent: { errno: 1213, code: 'ER_LOCK_DEADLOCK' },
        });
        let attemptsOnDeadlocked = 0;
        await consumer.withRetryPolicy({ ...FAST_RETRY, maxAttempts: 4 }).onRecord(async (m) => {
            if (m.json().deadlock) { attemptsOnDeadlocked++; throw deadlock(); }
            await insert(m);
        }).start();

        const next = uniq('next');
        await send(topic, [{ value: json({ deadlock: true }) }, { value: json({ msgId: next }) }]);

        await waitFor(async () => (await rowIds()).includes(next), 60000, 'the next message to be processed');
        expect(attemptsOnDeadlocked).toBe(4);
        expect(of(events, 'retry').map((e) => e.errorClass)).toEqual(['transient-message', 'transient-message', 'transient-message']);
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 2, 30000, 'offsets to be committed');

        const dead = await readRaw(broker.kafka, dlq, { readUncommitted: false });
        expect(dead).toHaveLength(1);
        expect(dead[0].headers['x-error-class']).toBe('exhausted');
        expect(dead[0].headers['x-error']).toMatch(/Deadlock/);
    });

    it('a database outage is retried until the database returns, and never dead-lettered', async () => {
        const { topic, dlq } = await topics();
        const { consumer, events, groupId } = strictConsumer(topic);
        await consumer.withRetryPolicy({ baseDelayMs: 50, maxDelayMs: 400, maxAttempts: 2 }).onRecord(insert).start();

        const before = uniq('before'), during = uniq('during');
        await send(topic, [{ value: json({ msgId: before }) }]);
        await waitFor(async () => (await rowIds()).includes(before), 60000, 'the first message');
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 1, 30000, 'first offset');

        await mysqlProxy.pause(); // the database is gone
        await send(topic, [{ value: json({ msgId: during }) }]);
        // many more retries than maxAttempts: infrastructure failures don't count against the message
        await waitFor(() => of(events, 'retry').filter((e) => e.errorClass === 'transient-infra').length >= 6, 60000, 'retries during the outage');
        expect(of(events, 'dlq')).toHaveLength(0);
        expect(await committedOffset(broker.kafka, groupId, topic)).toBe(1); // offset stays uncommitted

        await mysqlProxy.resume(); // the database is back
        await waitFor(async () => (await rowIds()).includes(during), 60000, 'the message to succeed once the database is back');
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 2, 30000, 'offset commit after recovery');
        expect(of(events, 'dlq')).toHaveLength(0);
        expect(await readRaw(broker.kafka, dlq, { readUncommitted: false })).toHaveLength(0);
    });

    it('tombstones and empty values are dead-lettered (never thrown) and the next message is processed', async () => {
        const { topic, dlq } = await topics();
        const { consumer, groupId } = strictConsumer(topic);
        const seen: string[] = [];
        await consumer.onRecord(async (m) => { seen.push(m.json().msgId); await insert(m); }).start();

        const good = uniq('good');
        await send(topic, [{ key: 'acct-1', value: null }, { key: 'acct-2', value: '' }, { key: 'acct-3', value: json({ msgId: good }) }]);

        await waitFor(async () => (await rowIds()).includes(good), 60000, 'the message after the tombstone');
        expect(seen).toEqual([good]); // the handler never saw the tombstone
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 3, 30000, 'offsets');

        const dead = await readRaw(broker.kafka, dlq, { readUncommitted: false });
        expect(dead.map((d) => d.key)).toEqual(['acct-1', 'acct-2']); // original keys preserved
        expect(dead[0].value).toBeUndefined();               // null stays null
        expect(dead.every((d) => d.headers['x-error-class'] === 'tombstone')).toBe(true);
    });

    it('a duplicate redelivery is acknowledged: no DLQ record, offset committed (unique constraint is the guard)', async () => {
        const { topic, dlq } = await topics();
        const { consumer, events, groupId } = strictConsumer(topic);
        let calls = 0;
        await consumer.withIdempotency({ keyFrom: (m) => m.json().msgId })
            .onRecord(async (m) => { calls++; await insert(m); }).start();

        const id = uniq('dup');
        await send(topic, [{ value: json({ msgId: id }) }, { value: json({ msgId: id }) }]); // same event delivered twice

        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 2, 60000, 'both offsets to be committed');
        expect((await rowIds()).filter((r) => r === id)).toHaveLength(1);
        expect(calls).toBe(2); // second insert ran and hit ER_DUP_ENTRY (1062)
        expect(of(events, 'processed').map((e) => e.outcome)).toEqual(['handled', 'duplicate']);
        expect(of(events, 'dlq')).toHaveLength(0);
        expect(await readRaw(broker.kafka, dlq, { readUncommitted: false })).toHaveLength(0);
    });

    it('alreadyDone skips the handler entirely and the message is acknowledged', async () => {
        const { topic, dlq } = await topics();
        const { consumer, events, groupId } = strictConsumer(topic);
        const handled: string[] = [];
        const done = new Set<string>();
        await consumer.withIdempotency({
            keyFrom: (m) => m.json().msgId,
            alreadyDone: (key) => done.has(key),
        }).onRecord(async (m) => { handled.push(m.idempotencyKey!); done.add(m.idempotencyKey!); await insert(m); }).start();

        const id = uniq('once');
        await send(topic, [{ value: json({ msgId: id }) }, { value: json({ msgId: id }) }]);
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 2, 60000, 'offsets');
        expect(handled).toEqual([id]);
        expect(of(events, 'processed').map((e) => e.outcome)).toEqual(['handled', 'already-done']);
        expect(await readRaw(broker.kafka, dlq, { readUncommitted: false })).toHaveLength(0);
    });

    it('a missing key is processed without a pre-check, unless the policy requires a key', async () => {
        const { topic, dlq } = await topics();
        const optional = strictConsumer(topic);
        const keys: Array<string | undefined> = [];
        await optional.consumer.withIdempotency({ alreadyDone: () => { throw new Error('must not be called without a key'); } })
            .onRecord(async (m) => { keys.push(m.idempotencyKey); await insert(m); }).start();
        const id = uniq('nokey');
        await send(topic, [{ value: json({ msgId: id }) }]); // no idempotency-key header, no Kafka key
        await waitFor(async () => (await committedOffset(broker.kafka, optional.groupId, topic)) === 1, 60000, 'offset');
        expect(keys).toEqual([undefined]);
        expect(await readRaw(broker.kafka, dlq, { readUncommitted: false })).toHaveLength(0);
        await optional.consumer.stop();

        const required = strictConsumer(topic);
        const handled: string[] = [];
        await required.consumer.withIdempotency({ requireKey: true }).onRecord(async (m) => { handled.push(m.json().msgId); }).start();
        await waitFor(async () => (await committedOffset(broker.kafka, required.groupId, topic)) === 1, 60000, 'offset');
        expect(handled).toEqual([]);
        const dead = await readRaw(broker.kafka, dlq, { readUncommitted: false });
        expect(dead).toHaveLength(1);
        expect(dead[0].headers).toMatchObject({ 'x-error-class': 'permanent', 'dlq-reason': 'missing-idempotency-key' });
    });

    // ------------------------------------------------------------------------------------------------ offsets / DLQ atomicity

    it('a DLQ transaction that aborts leaves neither a DLQ record nor a committed offset, and succeeds once the fault clears', async () => {
        const { topic, dlq } = await topics();
        const fault = { on: true };
        const faulty = newBroker();
        const makeProducer = faulty.kafka.producer.bind(faulty.kafka);
        faulty.kafka.producer = ((config: any) => {
            const producer = makeProducer(config);
            if (!config?.kafkaJS?.transactionalId) return producer;
            return overriding(producer, {
                transaction: async () => {
                    const tx = await producer.transaction();
                    // the record is already sent inside the transaction when the offset commit fails: it must be rolled back
                    return overriding(tx, { sendOffsets: async (a: any) => { if (fault.on) throw new Error('injected: sendOffsets failed'); return tx.sendOffsets(a); } });
                },
            });
        }) as any;

        const groupId = uniq('g');
        const events: ConsumerEvent[] = [];
        const processed: string[] = [];
        const consumer = track(new Consumer(faulty)).withGroupId(groupId).withTopic(topic).withStrictProcessing().withMysqlClassifier()
            .withRetryPolicy(FAST_RETRY).onEvent((e) => events.push(e))
            .onRecord(async (m) => { processed.push(m.json().msgId); await insert(m); });
        await consumer.start();

        const bad = uniq('poison'), after = uniq('after');
        await send(topic, [{ value: json({ msgId: bad, note: 'way-too-long-for-column' }) }, { value: json({ msgId: after }) }]);

        await waitFor(() => of(events, 'retry').filter((e) => e.phase === 'dlq').length >= 3, 60000, 'the DLQ transaction to be aborted a few times');
        expect(await readRaw(broker.kafka, dlq, { readUncommitted: false })).toHaveLength(0); // aborted: nothing visible
        expect(await committedOffset(broker.kafka, groupId, topic)).toBe(-1);                  // and nothing committed
        expect(processed).not.toContain(after);                                                // order kept: nothing skipped past it

        fault.on = false;
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 2, 60000, 'recovery');
        const dead = await readRaw(broker.kafka, dlq, { readUncommitted: false });
        expect(dead).toHaveLength(1); // exactly one record despite the aborted attempts
        expect(JSON.parse(dead[0].value!).msgId).toBe(bad);
        expect(await rowIds()).toContain(after);
    });

    it('a crash before the offset commit causes redelivery', async () => {
        const { topic } = await topics();
        const groupId = uniq('g');
        const handledBy: string[] = [];

        // process A: the handler runs and its DB transaction commits, then the process dies before the offset commit
        const crashing = newBroker();
        const makeConsumer = crashing.kafka.consumer.bind(crashing.kafka);
        crashing.kafka.consumer = ((config: any) => {
            const c = makeConsumer(config);
            return overriding(c, { commitOffsets: async () => { throw new KafkaJS.KafkaJSError('process died before commit'); } });
        }) as any;
        const a = track(new Consumer(crashing)).withGroupId(groupId).withTopic(topic).withStrictProcessing().withMysqlClassifier()
            .onRecord(async (m) => { handledBy.push(`A@${m.meta.offset}`); await insert(m); });
        await a.start();

        const id = uniq('payment');
        await send(topic, [{ value: json({ msgId: id }) }]);
        await waitFor(() => handledBy.length >= 1, 60000, 'process A to handle the message');
        await sleep(1500);
        expect(await committedOffset(broker.kafka, groupId, topic)).toBe(-1); // never committed
        await a.stop();

        // process B (same group) must get the same message again; the unique constraint is what absorbs the re-run
        const b = track(new Consumer(broker)).withGroupId(groupId).withTopic(topic).withStrictProcessing().withMysqlClassifier()
            .withIdempotency({ keyFrom: (m) => m.json().msgId })
            .onRecord(async (m) => { handledBy.push(`B@${m.meta.offset}`); await insert(m); });
        await b.start();
        await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 1, 60000, 'process B to commit');
        expect(handledBy).toEqual(['A@0', 'B@0']); // same offset, delivered twice
        expect((await rowIds()).filter((r) => r === id)).toHaveLength(1);
        await b.stop();
    });

    // ------------------------------------------------------------------------------------------------ gate

    describe('HALT gate', () => {
        const gated = (topic: string, isHalted: () => Promise<boolean>) => {
            const handled: string[] = [];
            const { consumer, groupId } = strictConsumer(topic);
            consumer.withGate(isHalted, { pollMs: 100 }).onRecord(async (m) => { handled.push(m.json().msgId); await insert(m); });
            return { consumer, groupId, handled };
        };

        it('halted: nothing is processed or committed, the session stays alive; cleared: processing resumes; stop() works while halted', async () => {
            const { topic } = await topics();
            const state = { halted: true };
            const { consumer, groupId, handled } = gated(topic, async () => state.halted);
            await consumer.start();

            const ids = [uniq('a'), uniq('b'), uniq('c')];
            await send(topic, ids.map((msgId) => ({ value: json({ msgId }) })));
            await sleep(4000);
            expect(handled).toEqual([]);
            expect(await committedOffset(broker.kafka, groupId, topic)).toBe(-1);
            // still a live, stable group member: heartbeats are flowing, so no rebalance
            const admin = broker.kafka.admin();
            await admin.connect();
            const { groups } = await admin.describeGroups([groupId]);
            await admin.disconnect();
            expect(groups[0]).toMatchObject({ state: KafkaJS.ConsumerGroupStates.STABLE });
            expect(groups[0].members).toHaveLength(1);

            state.halted = false;
            await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 3, 60000, 'processing to resume');
            expect(handled).toEqual(ids); // in order

            state.halted = true;
            await send(topic, [{ value: json({ msgId: uniq('d') }) }]);
            await sleep(1500);
            await expect(Promise.race([consumer.stop().then(() => 'stopped'), sleep(15000).then(() => 'hung')])).resolves.toBe('stopped');
        });

        it('a gate that throws counts as halted', async () => {
            const { topic } = await topics();
            const state = { broken: true };
            const { consumer, groupId, handled } = gated(topic, async () => { if (state.broken) throw new Error('gate store unreachable'); return false; });
            await consumer.start();
            const id = uniq('x');
            await send(topic, [{ value: json({ msgId: id }) }]);
            await sleep(3000);
            expect(handled).toEqual([]);
            expect(await committedOffset(broker.kafka, groupId, topic)).toBe(-1);

            state.broken = false; // the gate answers again, and says "go"
            await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic)) === 1, 60000, 'resume after the gate recovered');
            expect(handled).toEqual([id]);
        });

        it('a gate that is already halted at start() blocks the very first message', async () => {
            const { topic } = await topics();
            await send(topic, [{ value: json({ msgId: uniq('early') }) }]); // waiting in the topic before the consumer exists
            const { consumer, handled } = gated(topic, async () => true);
            await consumer.start();
            await sleep(3000);
            expect(handled).toEqual([]);
        });
    });

    // ------------------------------------------------------------------------------------------------ concurrency

    it('partitionsConsumedConcurrently: partitions run in parallel, order is kept inside each partition', async () => {
        const partitions = 4;
        const { topic } = await topics(partitions);
        const { consumer, groupId } = strictConsumer(topic);
        let running = 0, maxRunning = 0;
        const perPartition = new Map<number, { running: number; overlapped: boolean; order: number[] }>();
        await consumer.withPartitionConcurrency(partitions).onRecord(async (m) => {
            const st = perPartition.get(m.meta.partition) ?? { running: 0, overlapped: false, order: [] };
            perPartition.set(m.meta.partition, st);
            st.running++; running++; maxRunning = Math.max(maxRunning, running);
            if (st.running > 1) st.overlapped = true;
            st.order.push(Number(m.meta.offset));
            await sleep(1200);
            st.running--; running--;
        }).start();

        const messages: Array<{ partition: number; value: string }> = [];
        for (let p = 0; p < partitions; p++) for (let i = 0; i < 3; i++) messages.push({ partition: p, value: json({ msgId: uniq('m'), i }) });
        const t0 = Date.now();
        await send(topic, messages);
        await waitFor(() => [...perPartition.values()].reduce((n, s) => n + s.order.length, 0) === partitions * 3, 60000, 'all messages');
        await waitFor(() => running === 0, 10000, 'handlers to finish');

        expect(maxRunning).toBeGreaterThanOrEqual(3);                              // different partitions overlapped
        expect(Date.now() - t0).toBeLessThan(partitions * 3 * 1200 * 0.6);         // far faster than serial
        for (const st of perPartition.values()) {
            expect(st.overlapped).toBe(false);                                     // never two at once in one partition
            expect(st.order).toEqual([...st.order].sort((x, y) => x - y));         // offsets in order
        }
        for (let p = 0; p < partitions; p++) {
            await waitFor(async () => (await committedOffset(broker.kafka, groupId, topic, p)) === 3, 30000, `partition ${p} offsets`);
        }
    });
});
