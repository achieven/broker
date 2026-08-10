# @achieven/broker

Kafka broker module (`BrokerModule` / `BrokerService`) shared across bpm-music-runner services. Published as a private package on GitHub Packages.

## Installing in a consuming project

1. Add a `.npmrc` in the project root (or `~/.npmrc` for local dev):

   ```
   @achieven:registry=https://npm.pkg.github.com
   //npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
   ```

2. Set a `GITHUB_TOKEN` environment variable with a personal access token that has `read:packages` scope (in CI, the default `GITHUB_TOKEN` works if the consuming repo is also owned by `achieven`; otherwise use a PAT).

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
