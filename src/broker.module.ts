import { Module } from '@nestjs/common';
import { BrokerService } from './broker.service';
import { ConsumerModule } from './consumer/consumer.module';
import { ProducerModule } from './producer/producer.module';

@Module({
  providers: [BrokerService],
  exports: [BrokerService],
  imports: [ConsumerModule, ProducerModule]
})
export class BrokerModule {}
