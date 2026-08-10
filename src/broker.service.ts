import { Injectable } from '@nestjs/common';
import { Kafka } from 'kafkajs';

@Injectable()
export class BrokerService {
    public kafka: Kafka
    constructor() {
        const brokerUrl = `${process.env.BROKER_HOST || 'localhost'}:${Number(process.env.BROKER_PORT)|| 19092}`
        console.log("Creating Kafka with broker URL:", brokerUrl)
        this.kafka = new Kafka({
            brokers: [brokerUrl]
          })
        console.log("Kafka instance created successfully")
    }
}
