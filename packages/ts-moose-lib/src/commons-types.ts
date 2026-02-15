/**
 * @fileoverview Pure TypeScript types, interfaces, constants, and utility functions
 * extracted from commons.ts. This module has NO native dependencies (no Kafka,
 * no ClickHouse client, no fs) and is safe to import in serverless/Lambda environments.
 *
 * The full commons.ts re-exports everything from this module for backward compatibility.
 *
 * @module commons-types
 */

/**
 * Returns true if the value is a common truthy string: "1", "true", "yes", "on" (case-insensitive).
 */
function isTruthy(value: string | undefined): boolean {
  if (!value) return false;
  switch (value.trim().toLowerCase()) {
    case "1":
    case "true":
    case "yes":
    case "on":
      return true;
    default:
      return false;
  }
}

/**
 * Utility function for compiler-related logging that can be disabled via environment variable.
 * Set MOOSE_DISABLE_COMPILER_LOGS=true to suppress these logs (useful for testing environments).
 */
export const compilerLog = (message: string) => {
  if (!isTruthy(process.env.MOOSE_DISABLE_COMPILER_LOGS)) {
    console.log(message);
  }
};

export const antiCachePath = (path: string) =>
  `${path}?num=${Math.random().toString()}&time=${Date.now()}`;

export const getFileName = (filePath: string) => {
  const regex = /\/([^/]+)\.ts/;
  const matches = filePath.match(regex);
  if (matches && matches.length > 1) {
    return matches[1];
  }
  return "";
};

export type CliLogData = {
  message_type?: "Info" | "Success" | "Warning" | "Error" | "Highlight";
  action: string;
  message: string;
};

export const cliLog: (log: CliLogData) => void = (log) => {
  const level =
    log.message_type === "Error" ? "error"
    : log.message_type === "Warning" ? "warn"
    : "info";

  const structuredLog = {
    __moose_structured_log__: true,
    level,
    message: log.message,
    resource_type: "runtime",
    cli_action: log.action,
    cli_message_type: log.message_type ?? "Info",
    timestamp: new Date().toISOString(),
  };

  process.stderr.write(JSON.stringify(structuredLog) + "\n");
};

/**
 * Method to change .ts, .cts, and .mts to .js, .cjs, and .mjs
 * This is needed because 'import' does not support .ts, .cts, and .mts
 */
export function mapTstoJs(filePath: string): string {
  return filePath
    .replace(/\.ts$/, ".js")
    .replace(/\.cts$/, ".cjs")
    .replace(/\.mts$/, ".mjs");
}

export const MAX_RETRIES = 150;
export const MAX_RETRY_TIME_MS = 1000;
export const RETRY_INITIAL_TIME_MS = 100;

export const MAX_RETRIES_PRODUCER = 150;
export const RETRY_FACTOR_PRODUCER = 0.2;
// Means all replicas need to acknowledge the message
export const ACKs = -1;

/**
 * Interface for logging functionality
 */
export interface Logger {
  logPrefix: string;
  log: (message: string) => void;
  error: (message: string) => void;
  warn: (message: string) => void;
}

export const logError = (logger: Logger, e: Error): void => {
  logger.error(e.message);
  const stack = e.stack;
  if (stack) {
    logger.error(stack);
  }
};

export type KafkaClientConfig = {
  clientId: string;
  broker: string;
  securityProtocol?: string; // e.g. "SASL_SSL" or "PLAINTEXT"
  saslUsername?: string;
  saslPassword?: string;
  saslMechanism?: string; // e.g. "scram-sha-256", "plain"
};

/**
 * Kafka Producer type placeholder for serverless environments.
 * In non-serverless contexts, the actual Producer type from @514labs/kafka-javascript
 * is used via the full commons module. This interface mirrors the subset of the
 * Producer API used by the Stream SDK.
 */
export interface Producer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: {
    topic: string;
    messages: Array<{
      value: Buffer | string | null;
      key?: Buffer | string | null;
    }>;
  }): Promise<any>;
}
