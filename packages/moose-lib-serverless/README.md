# @bayoudhi/moose-lib-serverless

Serverless-compatible subset of [`@514labs/moose-lib`](https://www.npmjs.com/package/@514labs/moose-lib) for **AWS Lambda**, **Edge runtimes**, and other environments where native C++ modules cannot be compiled or loaded.

This package re-exports the pure-TypeScript surface of the Moose SDK — OlapTable, Stream, View, Workflow, sql helpers, ClickHouse type annotations, and more — without pulling in `@514labs/kafka-javascript`, `@temporalio/client`, or `redis`.

## Installation

```bash
npm install @bayoudhi/moose-lib-serverless
```

If you use `OlapTable<T>`, `Stream<T>`, or other generic Moose resources that require compile-time schema injection, you also need the compiler plugin dependencies:

```bash
npm install -D ts-patch typia typescript
```

## Compiler Plugin Setup

The Moose compiler plugin transforms generic resource declarations like `new OlapTable<MyType>(...)` at compile time, injecting JSON schemas, column definitions, and runtime validators. Without it, you'll get:

```
Supply the type param T so that the schema is inserted by the compiler plugin.
```

### 1. Configure `tsconfig.json`

Add the compiler plugin and typia transform to your `tsconfig.json`:

```json
{
  "compilerOptions": {
    "plugins": [
      {
        "transform": "@bayoudhi/moose-lib-serverless/dist/compilerPlugin.js"
      },
      {
        "transform": "typia/lib/transform"
      }
    ]
  }
}
```

### 2. Install ts-patch

```bash
npx ts-patch install
```

### 3. Build with `tspc` instead of `tsc`

```bash
npx tspc
```

Or add it to your `package.json` scripts:

```json
{
  "scripts": {
    "build": "tspc"
  }
}
```

> **Note**: `tspc` is a drop-in replacement for `tsc` that loads the compiler plugins defined in `tsconfig.json`. Standard `tsc` ignores the `plugins` array.

## Usage

### CommonJS (recommended for Lambda)

```js
const { OlapTable, Stream, sql } = require("@bayoudhi/moose-lib-serverless");
```

### ES Modules

```js
import { OlapTable, Stream, sql } from "@bayoudhi/moose-lib-serverless";
```

> **Note**: The CJS bundle is recommended for AWS Lambda because ESM top-level imports cannot be lazily deferred. The CJS bundle keeps the Kafka reference inside a lazy `__esm` block that never executes unless you explicitly call `getClickhouseClient()` or `getKafkaProducer()`.

## What's Included

| Export | Description |
| --- | --- |
| `OlapTable` | Define ClickHouse OLAP tables |
| `Stream` | Define streaming ingestion points |
| `View` | Define materialized/live views |
| `Workflow` | Define workflow steps |
| `sql` | Tagged template literal for SQL queries |
| `Key`, `JWT` | Type annotations for data model keys and JWT auth |
| ClickHouse column types | `Columns.String`, `Columns.Int32`, `Columns.DateTime`, etc. |
| `ConsumptionUtil`, `ApiUtil` | Utility types for consumption APIs |
| `registerDataSource` | Register external data source connectors |
| `getSecrets` | Retrieve secrets from the Moose secrets store |
| Utility functions | `compilerLog`, `cliLog`, `mapTstoJs`, `getFileName`, etc. |

## What's Excluded

These native/C++ dependencies are **not** bundled and will never be loaded:

| Dependency | Reason |
| --- | --- |
| `@514labs/kafka-javascript` | Native C++ module (`node-rdkafka`); crashes in Lambda |
| `@temporalio/client` | Native Rust bridge; not available in serverless |
| `redis` | TCP connection pooling incompatible with short-lived functions |

## Origin

This package is derived from [MooseStack](https://github.com/514-labs/moosestack) by [Fiveonefour Labs](https://www.fiveonefour.com/), published under the MIT license.

- **Original project**: https://github.com/514-labs/moosestack
- **Fork**: https://github.com/bayoudhi/moosestack (branch `feature/serverless-compatibility`)

## License

MIT — see [LICENSE](./LICENSE) for details.
