import { chainClassifiers, genericClassifier, markerClass } from './error-class';
import { mongoClassifier } from './mongo-classifier';
import { mysqlClassifier } from './mysql-classifier';

const named = (name: string, props: Record<string, unknown> = {}) => Object.assign(new Error(name), { name }, props);
/** What mysql2 / Sequelize produce: the errno lives on the driver error, wrapped by Sequelize in .parent / .original. */
const mysql = (errno: number) => named('SequelizeDatabaseError', { parent: { errno, code: `ER_${errno}` }, original: { errno } });
const direct = (errno: number) => Object.assign(new Error('mysql'), { errno });
const code = (c: string) => Object.assign(new Error(c), { code: c });

describe('mysqlClassifier', () => {
    it.each([1406, 1264, 1048, 1366, 1452])('errno %i is permanent', (errno) => {
        expect(mysqlClassifier(mysql(errno))).toBe('permanent');
        expect(mysqlClassifier(direct(errno))).toBe('permanent');
    });
    it.each([1213, 1205])('errno %i (deadlock / lock wait timeout) is transient-message', (errno) => {
        expect(mysqlClassifier(mysql(errno))).toBe('transient-message');
    });
    it.each([1290, 2013, 2006, 1053, 1047, 1317])('errno %i is transient-infra', (errno) => {
        expect(mysqlClassifier(mysql(errno))).toBe('transient-infra');
    });
    it('errno 1062 is duplicate, also as SequelizeUniqueConstraintError', () => {
        expect(mysqlClassifier(mysql(1062))).toBe('duplicate');
        expect(mysqlClassifier(named('SequelizeUniqueConstraintError'))).toBe('duplicate');
    });
    it.each(['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'PROTOCOL_CONNECTION_LOST', 'EAI_AGAIN', 'ENOTFOUND', 'EHOSTUNREACH'])(
        'code %s is transient-infra, even when wrapped', (c) => {
            expect(mysqlClassifier(code(c))).toBe('transient-infra');
            expect(mysqlClassifier(named('SequelizeConnectionError', { parent: code(c) }))).toBe('transient-infra');
        });
    it('pool errors without an errno are transient-infra', () => {
        expect(mysqlClassifier(named('SequelizeConnectionAcquireTimeoutError'))).toBe('transient-infra');
    });
    it('ZodError, SyntaxError and JSON.parse failures are permanent', () => {
        expect(mysqlClassifier(named('ZodError'))).toBe('permanent');
        expect(mysqlClassifier(new SyntaxError('x'))).toBe('permanent');
        expect(() => JSON.parse('{nope')).toThrow();
        try { JSON.parse('{nope'); } catch (e) { expect(mysqlClassifier(e)).toBe('permanent'); }
    });
    it('ioredis failures are transient-infra', () => {
        expect(mysqlClassifier(named('MaxRetriesPerRequestError'))).toBe('transient-infra');
        for (const m of ['Connection is closed.', 'READONLY You can\'t write against a read only replica', 'LOADING Redis is loading', 'CLUSTERDOWN The cluster is down']) {
            expect(mysqlClassifier(new Error(m))).toBe('transient-infra');
        }
    });
    it('anything else is unknown', () => {
        expect(mysqlClassifier(new Error('who knows'))).toBe('unknown');
        expect(mysqlClassifier(mysql(1146))).toBe('unknown');
        expect(mysqlClassifier('a string')).toBe('unknown');
        expect(mysqlClassifier(undefined)).toBe('unknown');
    });
});

describe('mongoClassifier', () => {
    const server = (c: number, extra: Record<string, unknown> = {}) => named('MongoServerError', { code: c, ...extra });
    it('11000 is duplicate (also inside bulk write errors, and legacy 11001)', () => {
        expect(mongoClassifier(server(11000))).toBe('duplicate');
        expect(mongoClassifier(server(11001))).toBe('duplicate');
        expect(mongoClassifier(named('MongoBulkWriteError', { writeErrors: [{ code: 11000 }] }))).toBe('duplicate');
    });
    it('WriteConflict 112 is transient-message, even with the TransientTransactionError label', () => {
        expect(mongoClassifier(server(112))).toBe('transient-message');
        expect(mongoClassifier(server(112, { errorLabels: ['TransientTransactionError'] }))).toBe('transient-message');
    });
    it.each([121, 2, 14])('code %i is permanent', (c) => expect(mongoClassifier(server(c))).toBe('permanent'));
    it.each([10107, 11600, 91, 189, 11602])('code %i is transient-infra', (c) => expect(mongoClassifier(server(c))).toBe('transient-infra'));
    it('network errors and the transient labels are transient-infra', () => {
        expect(mongoClassifier(named('MongoNetworkError'))).toBe('transient-infra');
        expect(mongoClassifier(named('MongoServerSelectionError'))).toBe('transient-infra');
        expect(mongoClassifier(code('ECONNRESET'))).toBe('transient-infra');
        expect(mongoClassifier(server(0, { errorLabels: ['RetryableWriteError'] }))).toBe('transient-infra');
        expect(mongoClassifier(named('MongoServerError', { hasErrorLabel: (l: string) => l === 'TransientTransactionError' }))).toBe('transient-infra');
    });
    it('ZodError / SyntaxError are permanent; unrecognised is unknown', () => {
        expect(mongoClassifier(named('ZodError'))).toBe('permanent');
        expect(mongoClassifier(server(99999))).toBe('unknown');
    });
});

describe('markers and chaining', () => {
    it('nonRetryable means permanent and retriable means transient-infra', () => {
        expect(markerClass({ nonRetryable: true })).toBe('permanent');
        expect(markerClass({ retriable: true })).toBe('transient-infra');
        expect(genericClassifier(Object.assign(new Error('x'), { retriable: true }))).toBe('transient-infra');
        expect(markerClass({ nonRetryable: false })).toBeUndefined();
    });
    it('the first classifier that recognises the error wins', () => {
        const first = (e: unknown) => ((e as Error).message === 'a' ? 'permanent' : 'unknown') as 'permanent' | 'unknown';
        const chained = chainClassifiers(first, () => 'transient-message');
        expect(chained(new Error('a'))).toBe('permanent');
        expect(chained(new Error('b'))).toBe('transient-message');
    });
});
