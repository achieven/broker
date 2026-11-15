/** Kafka header that carries the idempotency key (acts as a correlation id) from producer to consumer. */
export const IDEMPOTENCY_KEY_HEADER = 'idempotency-key';

/**
 * Broker <-> database idempotency.
 *
 * The consumer pre-reads with `exists()` to skip work that was already done. That read is only an optimisation:
 * the guarantee against concurrent duplicates is a UNIQUE constraint on the idempotency key column/field of the
 * document the consumer's logic writes (see `ensureSchema()`). The handler MUST write the key (`ctx.idempotencyKey`)
 * in the same atomic write/transaction as its business effects, so a concurrent duplicate fails with a
 * duplicate-key error (`isDuplicateKeyError()`), which the consumer sends straight to the DLQ without retries.
 */
export interface IdempotencyStore {
    /** Creates the unique constraint if it doesn't exist yet. Idempotent. */
    ensureSchema(): Promise<void>;
    /** True if a document with this idempotency key was already written. */
    exists(key: string): Promise<boolean>;
    /** True if the error was raised by the unique constraint on the idempotency key. */
    isDuplicateKeyError(err: unknown): boolean;
}
