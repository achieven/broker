import { Classifier, ErrorClass, chainClassifiers, errorChain, genericClassifier } from './error-class';

/** MongoDB server error codes (https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml). */
export const MONGO_CODE = {
    duplicate: new Set([11000, 11001]), // DuplicateKey (11001 is the legacy code)
    // `transient-message` WriteConflict: the write lost a race with another one; retrying normally succeeds
    transientMessage: new Set([112]),
    // the document is invalid for this collection; it will never be accepted
    permanent: new Set([
        121, // DocumentValidationFailure
        2,   // BadValue
        14,  // TypeMismatch
    ]),
    // the node can't serve writes right now: failover, shutdown, step-down
    transientInfra: new Set([
        10107, // NotWritablePrimary
        11600, // InterruptedAtShutdown
        91,    // ShutdownInProgress
        // Not in the original proposal, but they are the same failure family and show up during a failover:
        189,   // PrimarySteppedDown
        11602, // InterruptedDueToReplStateChange
        13435, // NotPrimaryNoSecondaryOk
        13436, // NotPrimaryOrSecondary
        6,     // HostUnreachable
        7,     // HostNotFound
        89,    // NetworkTimeout
        9001,  // SocketException
    ]),
};

const MONGO_NETWORK_NAMES = new Set([
    'MongoNetworkError', 'MongoNetworkTimeoutError', 'MongoServerSelectionError', 'MongoNotConnectedError',
    'MongoTopologyClosedError', 'MongoPoolClearedError', 'MongoPoolClosedError', 'MongoWaitQueueTimeoutError',
]);

const TRANSIENT_LABELS = ['TransientTransactionError', 'RetryableWriteError'];

function hasLabel(e: any, label: string): boolean {
    if (typeof e.hasErrorLabel === 'function') return e.hasErrorLabel(label);
    return Array.isArray(e.errorLabels) && e.errorLabels.includes(label);
}

function codesOf(e: any): number[] {
    // bulk writes nest the per-document failures in writeErrors
    return [e.code, ...(e.writeErrors ?? []).map((w: any) => w?.code ?? w?.err?.code)].filter((c) => typeof c === 'number');
}

function mongoMapping(err: unknown): ErrorClass {
    const chain = errorChain(err);
    const codes = chain.flatMap(codesOf);
    // duplicate wins over labels: a duplicate-key failure is final even inside a retryable write
    if (codes.some((c) => MONGO_CODE.duplicate.has(c))) return 'duplicate';
    if (codes.some((c) => MONGO_CODE.permanent.has(c))) return 'permanent';
    if (codes.some((c) => MONGO_CODE.transientMessage.has(c))) return 'transient-message';
    if (codes.some((c) => MONGO_CODE.transientInfra.has(c))) return 'transient-infra';
    if (chain.some((e) => MONGO_NETWORK_NAMES.has(e.name) || TRANSIENT_LABELS.some((l) => hasLabel(e, l)))) return 'transient-infra';
    return 'unknown';
}

/**
 * Default classifier for MongoDB, on top of the generic one.
 *
 *   duplicate          11000 (and legacy 11001)
 *   transient-message  WriteConflict 112
 *   permanent          121 DocumentValidationFailure, 2 BadValue, 14 TypeMismatch
 *   transient-infra    NotWritablePrimary 10107, InterruptedAtShutdown 11600, ShutdownInProgress 91 (+ other failover codes),
 *                      driver network errors, and the TransientTransactionError / RetryableWriteError labels
 */
export const mongoClassifier: Classifier = chainClassifiers(mongoMapping, genericClassifier);
