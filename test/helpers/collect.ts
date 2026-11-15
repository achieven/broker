import { BrokerService } from '../../src/broker.service';
import { Consumer, MessageContext } from '../../src/consumer/consumer';
import { uniq } from './cluster';

const started: Consumer[] = [];

/** Stops every consumer started through collect(), pass or fail, so a failed test can't leave consumers hammering the cluster. */
export async function stopAllConsumers() {
    await Promise.all(started.splice(0).map((c) => c.stop().catch(() => {})));
}

/** Registers a consumer so that afterEach stops it (stopping twice is harmless). */
export function track<T extends Consumer>(consumer: T): T {
    started.push(consumer);
    return consumer;
}

export interface Received { data: any; ctx: MessageContext }

/** Starts a Consumer on `topic` (optionally customised through `configure`) that records everything it is handed. */
export async function collect(
    broker: BrokerService,
    topic: string,
    configure: (c: Consumer) => Consumer = (c) => c,
    handler?: (data: any, ctx: MessageContext) => Promise<void>,
) {
    const received: Received[] = [];
    const consumer = configure(new Consumer(broker).withGroupId(uniq('group')).withTopic(topic)).onMessage(async (data, ctx) => {
        await handler?.(data, ctx);
        received.push({ data, ctx });
    });
    started.push(consumer);
    await consumer.start();
    return { received, consumer };
}
