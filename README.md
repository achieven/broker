# @achieven/broker

Kafka broker module (`BrokerModule` / `BrokerService`) shared across bpm-music-runner services. Published as a private package on GitHub Packages.
  

3. Install:

   ```
   npm install @achieven/broker
   ```

## Usage

```ts
import { Module } from '@nestjs/common';
import { BrokerModule } from '@achieven/broker';

@Module({
  imports: [BrokerModule],
})
export class AppModule {}
```

```ts
import { Injectable } from '@nestjs/common';
import { BrokerService, topics } from '@achieven/broker';

@Injectable()
export class MyService {
  constructor(private readonly broker: BrokerService) {}

  async publish() {
    const producer = this.broker.kafka.producer();
    await producer.connect();
    await producer.send({ topic: topics.clickNew, messages: [{ value: 'hi' }] });
  }
}
```

Configure the broker connection via env vars: `BROKER_HOST` (default `localhost`) and `BROKER_PORT` (default `19092`).

## Strict consumer (payments pipelines)

Opt in with `withStrictProcessing()`; without it the consumer behaves exactly as before.

```ts
const consumer = new Consumer(broker)
  .withGroupId('executor').withTopic('outbox.payments').withReadCommitted()
  .withStrictProcessing()
  .withMysqlClassifier()                         // or .withMongoClassifier(), or .withClassifier(myFn)
  .withIdempotency({ keyFrom: (m) => m.json().eventId, alreadyDone: (key) => isPastThisStage(key), onDuplicate: 'ack' })
  .withPartitionConcurrency(16)
  .withGate(async () => await isHalted())        // true = halted, a throw = halted
  .onEvent((e) => metrics.record(e))             // processed | dlq | retry
  .onRecord(async ({ value, key, headers, meta, json }) => { /* one DB transaction */ })
  .start();
```

* Offsets: `autoCommit` is off. Offset + 1 of a message is committed only after its handler returned. Brokers: `BROKER_HOSTS=a:9092,b:9092`.
* DLQ (`<topic>.dlq`, `withDlqTopic`): the record and the source offset are written in one Kafka transaction. Headers: `x-stage`, `x-error-class`, `x-error` (1000 chars), `x-source-topic|partition|offset`; key and value are preserved. `x-error-class` is `permanent`, `exhausted`, `tombstone` or `duplicate`.
* Tombstones and empty values are dead-lettered (`tombstone`), never thrown.
* Schema: `withIdempotency()` never touches the database schema. Only the older `with*IdempotencyCheck` / `withIdempotencyStore` call `ensureSchema()` at start.

| class | behaviour |
|---|---|
| `permanent` | DLQ immediately |
| `transient-message` / `unknown` | retried with jittered backoff up to `maxAttempts` (5), then DLQ `exhausted` |
| `transient-infra` | retried forever (base 100 ms, cap 10 s), never DLQ; `retry` event each time; stops when the consumer stops |
| `duplicate` | `onDuplicate: 'ack'` (default) acknowledges, `'dlq'` dead-letters. Without any idempotency policy it is dead-lettered |

Explicit markers on a thrown error beat every classifier: `nonRetryable: true` is permanent, `retriable: true` is transient-infra.

MySQL (errno from the error, `.original` or `.parent`): permanent 1406 1264 1048 1366 1452, ZodError, SyntaxError; transient-message 1213 1205; transient-infra 1290 2013 2006 1053 1047 1317, `ECONNRESET ECONNREFUSED ETIMEDOUT EPIPE PROTOCOL_CONNECTION_LOST EAI_AGAIN ENOTFOUND EHOSTUNREACH`, ioredis `MaxRetriesPerRequestError` / "Connection is closed" / READONLY / LOADING / CLUSTERDOWN; duplicate 1062.

MongoDB: duplicate 11000; transient-message WriteConflict 112; permanent 121, BadValue 2, TypeMismatch 14; transient-infra NotWritablePrimary 10107, InterruptedAtShutdown 11600, ShutdownInProgress 91 (plus the same-family 189, 11602, 13435, 13436, 6, 7, 89, 9001), driver network errors, `TransientTransactionError` / `RetryableWriteError` labels.

## Releasing a new version

1. Bump `version` in `package.json` (this package uses exact versions, no `^`/`~`).
2. Commit the bump.
3. Tag and push: `git tag broker-v<version> && git push origin broker-v<version>`.
4. The `Publish @achieven/broker` GitHub Actions workflow builds and publishes the package to GitHub Packages.

## Local development

```
npm install
npm run build   # compiles src -> dist
npm test
```
