import { stopAllConsumers } from './helpers/collect';
import { disconnectAllClients } from './helpers/cluster';

afterEach(stopAllConsumers);
afterAll(disconnectAllClients);
