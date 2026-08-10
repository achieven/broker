import { Producer } from 'kafkajs';
import { BrokerService } from '../broker.service';


export abstract class ProducerService {
    protected abstract producer: Producer;
    protected abstract topic: string;
    constructor(
        protected readonly brokerService: BrokerService,
    ) {
        
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
