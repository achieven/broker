import { Producer } from 'kafkajs';
import { BrokerService } from '../broker.service';


export abstract class ProducerService {
    protected producer: Producer;
    protected topic: string;
    constructor(
        protected readonly brokerService: BrokerService,
        topic: string,
    ) {
        const producer = this.brokerService.kafka.producer()
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
