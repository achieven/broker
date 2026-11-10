import { CompressionTypes } from 'kafkajs';
import { BrokerService } from '../broker.service';
import { ProducerService } from './producer.service';
import { DurableTopicService } from '../topic/durable-topic.service';

/**
 * Maximally durable producer, intended for the first hop of the queue where an event must never be lost.
 *
 * - acks=-1 (all): the leader replies only after every in-sync replica has the message.
 * - idempotent: retries can't create duplicates or reorder messages.
 * - maxInFlightRequests=1: strict ordering even across retries.
 * - aggressive retries: transient broker/network failures are retried instead of surfaced.
 * - send() throws on failure, so the caller knows the event was NOT persisted and can retry/nack.
 *
 * It can only write to a DurableTopicService, which guarantees (and validates at startup) replication.factor >= 3,
 * min.insync.replicas >= 2 and unclean.leader.election.enable=false, so "all" can't degrade to a single replica.
 */
export abstract class DurableProducerService extends ProducerService {
    constructor(brokerService: BrokerService, protected readonly durableTopic: DurableTopicService) {
        super(brokerService, durableTopic.name, {
            idempotent: true,
            maxInFlightRequests: 1,
            allowAutoTopicCreation: false, // a typo'd/missing topic must fail loudly, not be created with weak defaults
            retry: {
                retries: 20,
                initialRetryTime: 300,
                maxRetryTime: 30000,
            },
        });
    }

    async onModuleInit() {
        await this.durableTopic.ensure(); // topic must exist and be validated before we connect
        await super.onModuleInit();
    }

    async produce(message: any) {
        await this.producer.send({
            topic: this.topic,
            acks: -1, // all in-sync replicas
            timeout: 30000,
            compression: CompressionTypes.None,
            messages: [{ value: JSON.stringify(message) }],
        });
    }
}
