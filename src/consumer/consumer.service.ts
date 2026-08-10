import { BrokerService } from '../broker.service';
export abstract class ConsumerService {
    protected abstract callback: (data: any) => Promise<void>
    protected abstract groupId: string;
    protected abstract topic: string;
    constructor(protected readonly brokerService: BrokerService, callback: (data: any) => Promise<void>)   {}

    async onModuleInit() {
        const consumer = this.brokerService.kafka.consumer({ groupId: this.groupId })
        await consumer.connect()
        await consumer.subscribe({ topic: this.topic, fromBeginning: true })
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log('Received message:', message.value?.toString())
                try {
                    const value = message.value?.toString()
                    if (!value) throw new Error('no value')
                    const data = JSON.parse(value)
                    await this.callback(data)
                } catch (err) {
                    console.error('Error processing message:', err)
                    throw err
                }
            }
        })
    }


}
