import { BrokerService } from '../broker.service';
export abstract class ConsumerService {
    protected callback: (data: any) => Promise<void>
    protected groupId: string;
    protected topic: string;
    constructor(
        protected readonly brokerService: BrokerService,
        callback: (data: any) => Promise<void>,
        groupId: string,
        topic: string
    )   {
        this.callback = callback
        this.groupId = groupId
        this.topic = topic
    }

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
