import { hostname } from 'node:os';

/** A transactional id fences every other producer using it, so two live producers in one process must not share one. */
export const liveTransactionalIds = new Set<string>();

/** Stable per running instance: $PRODUCER_INSTANCE_ID, else $HOSTNAME (the pod name on Kubernetes), else the OS hostname. */
export function instanceId(): string {
    return process.env.PRODUCER_INSTANCE_ID || process.env.HOSTNAME || hostname();
}
