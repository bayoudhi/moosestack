/**
 * @fileoverview Serverless-compatible entry point for @514labs/moose-lib
 *
 * This module re-exports the full set of SDK classes, types, and utilities
 * needed for serverless environments (AWS Lambda, edge runtimes, etc.).
 *
 * Native dependencies (Kafka, Temporal, Redis) are present in the import
 * graph via commons.ts, but consumers of this entry point are expected to
 * use a bundler (like the tsup config in @bayoudhi/moose-lib-serverless)
 * that stubs those modules at build time.
 *
 * @module serverless
 */

// Re-export everything from browserCompatible (SDK classes, types, sql helpers)
export * from "./browserCompatible";
// Re-export types and utilities from commons
export type { CliLogData, KafkaClientConfig, Logger } from "./commons";
export {
  ACKs,
  antiCachePath,
  cliLog,
  compilerLog,
  getFileName,
  logError,
  MAX_RETRIES,
  MAX_RETRIES_PRODUCER,
  MAX_RETRY_TIME_MS,
  mapTstoJs,
  RETRY_FACTOR_PRODUCER,
  RETRY_INITIAL_TIME_MS,
} from "./commons";
// Export data source connector abstract class (pure TS)
export * from "./connectors/dataSource";
// Export secrets and runtime environment helpers (pure TS, no native deps)
export * from "./secrets";
// Export utility types and helpers (pure TS)
export * from "./utilities";
