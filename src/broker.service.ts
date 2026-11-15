import { Inject, Injectable, Optional } from '@nestjs/common';
import { KafkaJS } from '@confluentinc/kafka-javascript';

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

const LOG_LEVELS: Record<string, KafkaJS.logLevel> = {
    nothing: KafkaJS.logLevel.NOTHING,
    error: KafkaJS.logLevel.ERROR,
    warn: KafkaJS.logLevel.WARN,
    info: KafkaJS.logLevel.INFO,
    debug: KafkaJS.logLevel.DEBUG,
};

/** $KAFKAJS_LOG_LEVEL (nothing|error|warn|info|debug), the variable kafkajs itself honoured; unset keeps the client default. */
export function resolveLogLevel(env: NodeJS.ProcessEnv = process.env): KafkaJS.logLevel | undefined {
    return LOG_LEVELS[(env.KAFKAJS_LOG_LEVEL ?? '').toLowerCase()];
}

@Injectable()
export class BrokerService {
    public kafka: KafkaJS.Kafka
    constructor(@Optional() @Inject(BROKER_OPTIONS) options?: BrokerOptions) {
        const brokers = resolveBrokers(options)
        console.log("Creating Kafka with brokers:", brokers.join(','))
        const logLevel = resolveLogLevel()
        this.kafka = new KafkaJS.Kafka({
            kafkaJS: { brokers, ...(logLevel === undefined ? {} : { logLevel }) },
          })
        console.log("Kafka instance created successfully")
    }
}
