import { Producer } from 'kafkajs';
import { BrokerService } from '../broker.service';


export abstract class ProducerService {
    abstract producer: Producer;
    protected topic: string;
    constructor(
        protected readonly brokerService: BrokerService,
        topic: string,
    ) {
        this.topic = topic
    }

    async onModuleInit() {
        const producer = this.brokerService.kafka.producer()
        this.producer = producer
        await this.producer.connect()
    }

    async produce(topic: string, message: any) {
        await this.producer.send({
            topic: this.topic,
            messages: [{ value: JSON.stringify(message) }],
        })
    }
}
