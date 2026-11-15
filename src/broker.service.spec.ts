import { Test } from '@nestjs/testing';
import { BrokerService, resolveBrokers } from './broker.service';

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

describe('resolveBrokers', () => {
  it('defaults to localhost:19092', () => {
    expect(resolveBrokers(undefined, {})).toEqual(['localhost:19092']);
  });

  it('keeps supporting a single BROKER_HOST / BROKER_PORT', () => {
    expect(resolveBrokers(undefined, { BROKER_HOST: 'k', BROKER_PORT: '9092' })).toEqual(['k:9092']);
  });

  it('accepts a comma separated BROKER_HOSTS list, ignoring blanks and spaces', () => {
    expect(resolveBrokers(undefined, { BROKER_HOSTS: 'a:9092, b:9092 ,,c:9092', BROKER_HOST: 'ignored' }))
      .toEqual(['a:9092', 'b:9092', 'c:9092']);
  });

  it('prefers an explicit array over the environment', () => {
    expect(resolveBrokers({ brokers: ['x:1', 'y:2'] }, { BROKER_HOSTS: 'a:9092' })).toEqual(['x:1', 'y:2']);
  });
});
