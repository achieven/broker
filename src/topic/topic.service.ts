import { KafkaJS } from '@confluentinc/kafka-javascript';
import { BrokerService } from '../broker.service';

export interface TopicOptions {
    numPartitions?: number;
    replicationFactor?: number;
    /** Topic-level configs, e.g. { 'min.insync.replicas': '2' } */
    configs?: Record<string, string>;
}

export abstract class TopicService {
    protected readonly options: Required<TopicOptions>;
    private ensured?: Promise<void>;
    /** True when the last ensure() created the topic (so its configs are exactly the ones requested), false when it pre-existed. */
    protected created = false;

    constructor(
        protected readonly brokerService: BrokerService,
        public readonly name: string,
        options: TopicOptions = {},
    ) {
        this.options = {
            numPartitions: options.numPartitions ?? 1,
            replicationFactor: options.replicationFactor ?? 1,
            configs: options.configs ?? {},
        };
    }

    async onModuleInit() {
        await this.ensure();
    }

    /** Idempotent: creates the topic if missing, then validates it. Safe to call from several places. */
    ensure(): Promise<void> {
        if (!this.ensured) {
            this.ensured = this.doEnsure().catch((err) => {
                this.ensured = undefined; // allow retry after a failure
                throw err;
            });
        }
        return this.ensured;
    }

    private async doEnsure() {
        const admin = this.brokerService.kafka.admin();
        await admin.connect();
        try {
            const topic: KafkaJS.ITopicConfig = {
                topic: this.name,
                numPartitions: this.options.numPartitions,
                replicationFactor: this.options.replicationFactor,
                configEntries: Object.entries(this.options.configs).map(([name, value]) => ({ name, value })),
            };
            // true = created now, false = it already existed. We don't wait for leaders in createTopics; we do the
            // waiting ourselves below, because metadata may not have propagated to every broker yet.
            this.created = await admin.createTopics({ topics: [topic] });
            // Right after creation, metadata may not have propagated to every broker yet; wait for it to become visible.
            await retryWhileNotVisible(async () => {
                await admin.fetchTopicMetadata({ topics: [this.name] });
                await this.validate(admin);
            });
        } finally {
            await admin.disconnect();
        }
    }

    /** Hook: throw if the (possibly pre-existing) topic doesn't meet requirements. */
    protected async validate(_admin: KafkaJS.Admin): Promise<void> {}

    protected async getMinReplicationFactor(admin: KafkaJS.Admin): Promise<number> {
        const topics = await admin.fetchTopicMetadata({ topics: [this.name] });
        const partitions = topics[0]?.partitions ?? [];
        return Math.min(...partitions.map((p) => p.replicas.length));
    }
}

const NOT_YET_VISIBLE_TYPES = ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_FOR_PARTITION'];
const NOT_YET_VISIBLE_CODES: number[] = [
    KafkaJS.ErrorCodes.ERR_UNKNOWN_TOPIC_OR_PART,
    KafkaJS.ErrorCodes.ERR_LEADER_NOT_AVAILABLE,
    KafkaJS.ErrorCodes.ERR_NOT_LEADER_FOR_PARTITION,
];

async function retryWhileNotVisible<T>(fn: () => Promise<T>, timeoutMs = 30000): Promise<T> {
    const start = Date.now();
    for (;;) {
        try {
            return await fn();
        } catch (err: any) {
            if (!(NOT_YET_VISIBLE_TYPES.includes(err?.type) || NOT_YET_VISIBLE_CODES.includes(err?.code)) || Date.now() - start > timeoutMs) throw err;
            await new Promise((resolve) => setTimeout(resolve, 300));
        }
    }
}
