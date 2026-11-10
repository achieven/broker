import { ConsumerService } from './consumer.service';

/**
 * Consumer for topics written transactionally: read_committed isolation, so it only sees records
 * from committed transactions (aborted and still-open transactions are skipped/held back).
 */
export abstract class TransactionDependantConsumerService extends ConsumerService {
    protected readUncommitted = false;
}
