import { IdempotencyStore } from './idempotency-store';

/** Structural subset of a Sequelize Model class, so this package doesn't depend on sequelize. */
export interface SequelizeModelLike {
    findOne(options: any): Promise<unknown | null>;
    getTableName(): any;
    rawAttributes: Record<string, any>;
    sync?(): Promise<unknown>;
    sequelize?: { getQueryInterface(): any };
}

export interface MysqlIdempotencyOptions {
    /** Model attribute holding the idempotency key. Default: `idempotencyKey`. */
    attribute?: string;
}

export function mysqlIdempotencyStore(model: SequelizeModelLike, options: MysqlIdempotencyOptions = {}): IdempotencyStore {
    const attribute = options.attribute ?? 'idempotencyKey';
    return {
        async ensureSchema() {
            const attr = model.rawAttributes[attribute];
            if (!attr) {
                throw new Error(`Model has no "${attribute}" attribute; add it (STRING, allowNull: false) to the model first`);
            }
            const column: string = attr.field ?? attribute;
            const queryInterface = model.sequelize?.getQueryInterface();
            if (!queryInterface) throw new Error('Model is not bound to a Sequelize instance');

            await model.sync?.(); // CREATE TABLE IF NOT EXISTS, so the table is there before we inspect it
            const table = model.getTableName();
            const tableName = typeof table === 'string' ? table : table.tableName;
            const indexes: any[] = await queryInterface.showIndex(table);
            const hasUnique = indexes.some(
                (i) => i.unique && i.fields?.length === 1 && i.fields[0].attribute === column,
            );
            if (!hasUnique) {
                await queryInterface.addIndex(table, [column], { unique: true, name: `uq_${tableName}_${column}` });
            }
        },
        async exists(key) {
            const row = await model.findOne({ where: { [attribute]: key }, attributes: [attribute], raw: true });
            return row != null;
        },
        isDuplicateKeyError(err: any) {
            const cause = err?.original ?? err?.parent ?? err;
            return (
                err?.name === 'SequelizeUniqueConstraintError' ||
                cause?.code === 'ER_DUP_ENTRY' ||
                cause?.errno === 1062
            );
        },
    };
}
