import { Inject, Injectable, Optional } from '@nestjs/common';
import { Kafka } from 'kafkajs';

/** Injection token for `BrokerOptions`, for Nest users who want to pass them through DI. */
export const BROKER_OPTIONS = 'BROKER_OPTIONS';

export interface BrokerOptions {
    /** Bootstrap brokers as `host:port`. Wins over the environment variables. */
    brokers?: string[];
}

/**
 * Bootstrap broker list, in order of precedence:
 *   1. `options.brokers`
 *   2. $BROKER_HOSTS, comma separated: `kafka-1:9092,kafka-2:9092,kafka-3:9092`
 *   3. $BROKER_HOST + $BROKER_PORT (default localhost:19092), the original single-broker setting
 */
export function resolveBrokers(options?: BrokerOptions, env: NodeJS.ProcessEnv = process.env): string[] {
    const clean = (list: string[]) => list.map((b) => b.trim()).filter(Boolean);
    const fromOptions = clean(options?.brokers ?? []);
    if (fromOptions.length) return fromOptions;
    const fromEnv = clean((env.BROKER_HOSTS ?? '').split(','));
    if (fromEnv.length) return fromEnv;
    return [`${env.BROKER_HOST || 'localhost'}:${Number(env.BROKER_PORT) || 19092}`];
}

@Injectable()
export class BrokerService {
    public kafka: Kafka
    constructor(@Optional() @Inject(BROKER_OPTIONS) options?: BrokerOptions) {
        const brokers = resolveBrokers(options)
        console.log("Creating Kafka with brokers:", brokers.join(','))
        this.kafka = new Kafka({
            brokers
          })
        console.log("Kafka instance created successfully")
    }
}
