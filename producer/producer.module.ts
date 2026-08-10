import { Module } from '@nestjs/common';
import { BrokerModule } from '../broker/broker.module';
import { DbModule } from '../db/db.module';
import { ModelsModule } from '../models/models.module';

@Module({
  imports: [BrokerModule, DbModule, ModelsModule],
  exports: [BrokerModule, DbModule, ModelsModule],
})
export class RelayModule {}
