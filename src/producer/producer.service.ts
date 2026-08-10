import { Producer } from 'kafkajs';
import { BrokerService } from '../broker.service';


export abstract class ProducerService {
    protected abstract producer: Producer;
    constructor(
        protected readonly brokerService: BrokerService,
    ) {}

    async onModuleInit() {
        const producer = this.brokerService.kafka.producer()
        await producer.connect()
    }

    async produce(topic: string, message: any) {
        await this.producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify(message) }],
        })
    }
}
