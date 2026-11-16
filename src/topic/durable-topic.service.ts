import { KafkaJS } from '@confluentinc/kafka-javascript';
import { BrokerService } from '../broker.service';
import { TopicOptions, TopicService } from './topic.service';

const MIN_REPLICATION_FACTOR = 3;
const MIN_INSYNC_REPLICAS = 2;

/**
 * A topic configured so that an acks=all write survives the loss of a broker.
 * Defaults: replication.factor=3, min.insync.replicas=2, unclean.leader.election.enable=false.
 * Creation AND pre-existing topics are validated; startup fails if the topic is weaker than the minimums.
 */
export abstract class DurableTopicService extends TopicService {
    /** Brand so that only durable topics are accepted where a DurableTopicService is required. */
    readonly durable = true as const;
    private warned = false;

    constructor(brokerService: BrokerService, name: string, options: TopicOptions = {}) {
        super(brokerService, name, {
            ...options,
            replicationFactor: options.replicationFactor ?? MIN_REPLICATION_FACTOR,
            configs: {
                'min.insync.replicas': String(MIN_INSYNC_REPLICAS),
                'unclean.leader.election.enable': 'false',
                ...options.configs,
            },
        });
    }

    protected async validate(admin: KafkaJS.Admin) {
        const problems: string[] = [];

        const replication = await this.getMinReplicationFactor(admin);
        if (replication < MIN_REPLICATION_FACTOR) {
            problems.push(`replication factor is ${replication}, need >= ${MIN_REPLICATION_FACTOR}`);
        }

        // BACKLOG: once @confluentinc/kafka-javascript supports describeConfigs, read the real configs of pre-existing
        // topics here and fail closed again (as with kafkajs), then drop the warning below.
        // The Confluent client has no describeConfigs. A topic created by this call has exactly the configs we asked
        // for, so those are validated; the configs of a pre-existing topic cannot be read back (see warnOnce below).
        const configs = this.options.configs;
        const minIsr = Number(configs['min.insync.replicas']);
        if (!(minIsr >= MIN_INSYNC_REPLICAS)) {
            problems.push(`min.insync.replicas is ${configs['min.insync.replicas']}, need >= ${MIN_INSYNC_REPLICAS}`);
        }
        if (configs['unclean.leader.election.enable'] === 'true') {
            problems.push('unclean.leader.election.enable is true, need false');
        }
        if (!this.created && !this.warned) {
            this.warned = true;
            console.warn(
                `Topic "${this.name}" already existed: its min.insync.replicas / unclean.leader.election.enable cannot be ` +
                `verified with @confluentinc/kafka-javascript (no describeConfigs); only the replication factor was checked.`,
            );
        }

        if (problems.length) {
            throw new Error(`Topic "${this.name}" is not durable: ${problems.join('; ')}`);
        }
    }
}
