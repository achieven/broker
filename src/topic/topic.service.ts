import { Admin, ITopicConfig, ConfigResourceTypes } from 'kafkajs';
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
            const topic: ITopicConfig = {
                topic: this.name,
                numPartitions: this.options.numPartitions,
                replicationFactor: this.options.replicationFactor,
                configEntries: Object.entries(this.options.configs).map(([name, value]) => ({ name, value })),
            };
            await admin.createTopics({ topics: [topic], waitForLeaders: true });
            await this.validate(admin);
        } finally {
            await admin.disconnect();
        }
    }

    /** Hook: throw if the (possibly pre-existing) topic doesn't meet requirements. */
    protected async validate(_admin: Admin): Promise<void> {}

    protected async getTopicConfigs(admin: Admin): Promise<Record<string, string>> {
        const { resources } = await admin.describeConfigs({
            includeSynonyms: false,
            resources: [{ type: ConfigResourceTypes.TOPIC, name: this.name }],
        });
        const result: Record<string, string> = {};
        for (const entry of resources[0]?.configEntries ?? []) {
            result[entry.configName] = entry.configValue;
        }
        return result;
    }

    protected async getMinReplicationFactor(admin: Admin): Promise<number> {
        const { topics } = await admin.fetchTopicMetadata({ topics: [this.name] });
        const partitions = topics[0]?.partitions ?? [];
        return Math.min(...partitions.map((p) => p.replicas.length));
    }
}
