import { RetryPolicy } from './types';

export const DEFAULT_RETRY: Required<RetryPolicy> = { maxAttempts: 5, baseDelayMs: 100, maxDelayMs: 10_000 };

/** Exponential backoff with equal jitter: half fixed, half random, so a thundering herd spreads out but never waits ~0. */
export function backoffDelay(attempt: number, policy: Required<RetryPolicy>, random = Math.random): number {
    const exp = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** Math.min(attempt - 1, 30));
    return Math.round(exp / 2 + random() * (exp / 2));
}

/** Serialises async tasks: a kafkajs transactional producer can run only one transaction at a time. */
export class Mutex {
    private tail: Promise<unknown> = Promise.resolve();
    run<T>(task: () => Promise<T>): Promise<T> {
        const result = this.tail.then(task, task);
        this.tail = result.catch(() => {});
        return result;
    }
}
