import { Classifier, ErrorClass, chainClassifiers, errorChain, genericClassifier, mysqlErrno } from './error-class';

/** MySQL / MariaDB server error numbers (https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html). */
export const MYSQL_ERRNO = {
    // the row can never be written as is
    permanent: new Set([
        1406, // ER_DATA_TOO_LONG
        1264, // ER_WARN_DATA_OUT_OF_RANGE
        1048, // ER_BAD_NULL_ERROR
        1366, // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
        1452, // ER_NO_REFERENCED_ROW_2 (foreign key)
    ]),
    // the statement lost a race; running it again normally succeeds
    transientMessage: new Set([
        1213, // ER_LOCK_DEADLOCK
        1205, // ER_LOCK_WAIT_TIMEOUT
    ]),
    // the server/connection is unavailable
    transientInfra: new Set([
        1290, // ER_OPTION_PREVENTS_STATEMENT (read-only / super-read-only after a failover)
        2013, // CR_SERVER_LOST
        2006, // CR_SERVER_GONE_ERROR
        1053, // ER_SERVER_SHUTDOWN
        1047, // ER_UNKNOWN_COM_ERROR (server not ready during startup)
        1317, // ER_QUERY_INTERRUPTED
    ]),
    duplicate: new Set([1062]), // ER_DUP_ENTRY
};

/** Sequelize errors that carry no errno (raised by the pool, before a query reaches the server). */
const SEQUELIZE_INFRA_NAMES = new Set([
    'SequelizeConnectionError', 'SequelizeConnectionRefusedError', 'SequelizeHostNotFoundError',
    'SequelizeHostNotReachableError', 'SequelizeConnectionTimedOutError', 'SequelizeConnectionAcquireTimeoutError',
    'SequelizeAccessDeniedError',
]);

const CLOSED_CONNECTION = /Can't add new command when connection is in closed state|Connection lost|connection is closed/i;

function mysqlMapping(err: unknown): ErrorClass {
    const errno = mysqlErrno(err);
    if (errno !== undefined) {
        if (MYSQL_ERRNO.duplicate.has(errno)) return 'duplicate';
        if (MYSQL_ERRNO.permanent.has(errno)) return 'permanent';
        if (MYSQL_ERRNO.transientMessage.has(errno)) return 'transient-message';
        if (MYSQL_ERRNO.transientInfra.has(errno)) return 'transient-infra';
    }
    if (errorChain(err).some((e) => e.name === 'SequelizeUniqueConstraintError')) return 'duplicate';
    if (errorChain(err).some((e) => SEQUELIZE_INFRA_NAMES.has(e.name))) return 'transient-infra';
    // mysql2 on a connection that died under us: no errno, but it flags the error `fatal` / words it like this
    if (errorChain(err).some((e) => (e.fatal === true && mysqlErrno(e) === undefined) || CLOSED_CONNECTION.test(String(e.message ?? '')))) {
        return 'transient-infra';
    }
    return 'unknown';
}

/**
 * Default classifier for MySQL (errno read from the error, `err.original` or `err.parent`), on top of the generic one
 * (markers, ZodError/SyntaxError -> permanent, network codes and ioredis failures -> transient-infra).
 * Markers (`nonRetryable` / `retriable`) are applied by the consumer before any classifier.
 */
export const mysqlClassifier: Classifier = chainClassifiers(mysqlMapping, genericClassifier);
