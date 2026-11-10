import { Producer, ProducerConfig } from 'kafkajs';
import { BrokerService } from '../broker.service';


export abstract class ProducerService {
    protected producer: Producer;
    protected topic: string;
    constructor(
        protected readonly brokerService: BrokerService,
        topic: string,
        config: ProducerConfig = {},
    ) {
        const producer = this.brokerService.kafka.producer(config)
        this.producer = producer
        this.topic = topic
    }

    async onModuleInit() {
        await this.producer.connect()
    }

    async produce(topic: string, message: any) {
        await this.producer.send({
            topic: this.topic,
            messages: [{ value: JSON.stringify(message) }],
        })
    }
}
