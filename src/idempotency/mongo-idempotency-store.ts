import { IdempotencyStore } from './idempotency-store';

/** Structural subset of a `mongodb` Collection, so this package doesn't depend on the driver. */
export interface MongoCollectionLike {
    findOne(filter: any, options?: any): Promise<unknown | null>;
    createIndex(spec: any, options?: any): Promise<unknown>;
}

export interface MongoIdempotencyOptions {
    /** Field holding the idempotency key in the collection's documents. Default: `idempotencyKey`. */
    field?: string;
}

export function mongoIdempotencyStore(collection: MongoCollectionLike, options: MongoIdempotencyOptions = {}): IdempotencyStore {
    const field = options.field ?? 'idempotencyKey';
    return {
        async ensureSchema() {
            await collection.createIndex({ [field]: 1 }, { unique: true, name: `uq_${field}` });
        },
        async exists(key) {
            const doc = await collection.findOne({ [field]: key }, { projection: { _id: 1 } });
            return doc != null;
        },
        isDuplicateKeyError(err: any) {
            // 11000 = DuplicateKey (also 11001 for legacy servers); bulk writes nest the code in writeErrors.
            const codes = [err?.code, ...(err?.writeErrors ?? []).map((e: any) => e?.code ?? e?.err?.code)];
            return codes.some((c) => c === 11000 || c === 11001);
        },
    };
}
