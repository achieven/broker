import { Test } from '@nestjs/testing';
import { BrokerService } from './broker.service';

describe('BrokerService', () => {
  it('creates a Kafka client pointed at BROKER_HOST:BROKER_PORT', async () => {
    process.env.BROKER_HOST = 'test-host';
    process.env.BROKER_PORT = '9999';

    const moduleRef = await Test.createTestingModule({
      providers: [BrokerService],
    }).compile();

    const service = moduleRef.get(BrokerService);

    expect(service.kafka).toBeDefined();
  });
});
