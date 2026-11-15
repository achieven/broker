/**
 * How the consumer reacts to a failure while processing one message.
 *
 *  - permanent          the message can never succeed (bad data, constraint violation): DLQ immediately, no retries
 *  - transient-message  this message hit a transient conflict (deadlock, lock timeout): retry with backoff,
 *                       at most `maxAttempts` times, then DLQ (`x-error-class: exhausted`)
 *  - transient-infra    a dependency is down (database, redis, network): retry with backoff forever, never DLQ.
 *                       The offset stays uncommitted, so nothing is lost; it stops only when the consumer stops
 *  - duplicate          another worker already did this work (unique constraint): see `idempotency.onDuplicate`
 *  - unknown            not recognised: treated like transient-message (bounded retries, then DLQ)
 */
export type ErrorClass = 'permanent' | 'transient-message' | 'transient-infra' | 'duplicate' | 'unknown';

/** Maps a thrown error to an ErrorClass. Return 'unknown' when you don't recognise the error. */
export type Classifier = (err: unknown) => ErrorClass;

/** Follows `.original` / `.parent` (Sequelize) and `.cause`, so the driver's own error is inspected. */
export function errorChain(err: unknown): any[] {
    const chain: any[] = [];
    const queue: any[] = [err];
    while (queue.length && chain.length < 6) {
        const e = queue.shift();
        if (e == null || typeof e !== 'object' || chain.includes(e)) continue;
        chain.push(e);
        queue.push(e.original, e.parent, e.cause);
    }
    return chain;
}

/** The numeric errno (MySQL) found anywhere in the chain. Negative numbers are libuv errnos, not MySQL's. */
export function mysqlErrno(err: unknown): number | undefined {
    for (const e of errorChain(err)) {
        if (typeof e.errno === 'number' && e.errno > 0) return e.errno;
    }
    return undefined;
}

/** All string codes in the chain (e.g. ECONNRESET, ER_DUP_ENTRY, PROTOCOL_CONNECTION_LOST). */
export function errorCodes(err: unknown): string[] {
    return errorChain(err).map((e) => e.code).filter((c): c is string => typeof c === 'string');
}

/** Explicit markers on the thrown error always win over any classifier: `nonRetryable` -> permanent, `retriable` -> transient-infra. */
export function markerClass(err: any): ErrorClass | undefined {
    if (err?.nonRetryable === true) return 'permanent';
    if (err?.retriable === true) return 'transient-infra';
    return undefined;
}

/** Combines classifiers: the first one that recognises the error (anything but 'unknown') wins. */
export function chainClassifiers(...classifiers: Classifier[]): Classifier {
    return (err) => {
        for (const classify of classifiers) {
            const c = classify(err);
            if (c !== 'unknown') return c;
        }
        return 'unknown';
    };
}

const NETWORK_CODES = new Set([
    'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'PROTOCOL_CONNECTION_LOST',
    'EAI_AGAIN', 'ENOTFOUND', 'EHOSTUNREACH',
]);

/** Network-level failures reported by Node sockets and the MySQL/Mongo/redis drivers. */
export function isNetworkError(err: unknown): boolean {
    return errorCodes(err).some((c) => NETWORK_CODES.has(c));
}

const REDIS_MESSAGE = /Connection is closed|READONLY|LOADING|CLUSTERDOWN/i;

/** ioredis: out of retries, connection closed, replica promoted/loading, cluster down. */
export function isRedisInfraError(err: unknown): boolean {
    return errorChain(err).some((e) => e.name === 'MaxRetriesPerRequestError' || REDIS_MESSAGE.test(String(e.message ?? '')));
}

/** Data errors independent of any database: validation (ZodError) and malformed JSON. */
export function isDataError(err: unknown): boolean {
    return errorChain(err).some((e) => e.name === 'ZodError' || e instanceof SyntaxError || e.name === 'SyntaxError');
}

/**
 * Database-agnostic baseline, always the last classifier in the chain:
 * markers, bad data (ZodError / SyntaxError) and network/redis outages. Everything else is 'unknown'.
 */
export const genericClassifier: Classifier = (err) => {
    const marker = markerClass(err);
    if (marker) return marker;
    if (isDataError(err)) return 'permanent';
    if (isNetworkError(err) || isRedisInfraError(err)) return 'transient-infra';
    return 'unknown';
};
