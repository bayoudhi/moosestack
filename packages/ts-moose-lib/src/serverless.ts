/**
 * @fileoverview Serverless-compatible entry point for @514labs/moose-lib
 *
 * This module provides a serverless/Lambda-safe subset of the moose-lib functionality.
 * It excludes all native dependencies (Kafka, Temporal, Redis) and only exports:
 * - Pure TypeScript SDK classes (OlapTable, Stream, Workflow, etc.)
 * - ClickHouse type annotations
 * - SQL helpers
 * - Registry functions
 * - Secrets/config helpers
 * - Data source connectors
 *
 * IMPORTANT: Streaming functionality (Stream.send()) and workflow execution
 * will fail at runtime unless you install the optional peer dependencies:
 * - @514labs/kafka-javascript (for Kafka/Redpanda streaming)
 * - @temporalio/* (for workflow execution)
 *
 * For AWS Lambda environments, you typically only need:
 * - OlapTable for ClickHouse data ingestion
 * - Type annotations for data modeling
 * - SQL helpers for queries
 *
 * @module serverless
 */

// Re-export everything from browserCompatible (SDK classes, types, sql helpers)
export * from "./browserCompatible";
// Re-export pure types/utilities from commons-types (no native deps)
export type { CliLogData, KafkaClientConfig, Logger } from "./commons-types";
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
} from "./commons-types";
// Export data source connector abstract class (pure TS)
export * from "./connectors/dataSource";
// Export secrets and runtime environment helpers (pure TS, no native deps)
export * from "./secrets";
// Export utility types and helpers (pure TS)
export * from "./utilities";
