// Re-export all pure TypeScript types, interfaces, and utilities from commons-types
// This maintains backward compatibility for existing imports from "./commons"
export * from "./commons-types";

import { KafkaJS } from "@514labs/kafka-javascript";
import type { SASLOptions } from "@514labs/kafka-javascript/types/kafkajs";
import { createClient } from "@clickhouse/client";
import {
  existsSync,
  readdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "fs";
import nodePath from "path";
// Import constants and types for local use in this file
import {
  ACKs,
  MAX_RETRIES,
  MAX_RETRIES_PRODUCER,
  MAX_RETRY_TIME_MS,
  RETRY_INITIAL_TIME_MS,
} from "./commons-types";

const { Kafka } = KafkaJS;
type Kafka = KafkaJS.Kafka;
type Consumer = KafkaJS.Consumer;
// Export the actual Producer type from Kafka (overrides the interface from commons-types)
export type Producer = KafkaJS.Producer;

interface ClientConfig {
  username: string;
  password: string;
  database: string;
  useSSL: string;
  host: string;
  port: string;
}

export const getClickhouseClient = ({
  username,
  password,
  database,
  useSSL,
  host,
  port,
}: ClientConfig) => {
  const protocol =
    useSSL === "1" || useSSL.toLowerCase() === "true" ? "https" : "http";
  console.log(`Connecting to Clickhouse at ${protocol}://${host}:${port}`);
  return createClient({
    url: `${protocol}://${host}:${port}`,
    username: username,
    password: password,
    database: database,
    application: "moose",
    // Note: wait_end_of_query is configured per operation type, not globally
    // to preserve SELECT query performance while ensuring INSERT/DDL reliability
  });
};

/**
 * Walks a directory recursively and returns all files matching the given extensions.
 * Skips node_modules directories.
 *
 * @param dir - Directory to walk
 * @param extensions - File extensions to include (e.g., [".js", ".mjs"])
 * @returns Array of file paths
 */
function walkDirectory(dir: string, extensions: string[]): string[] {
  const results: string[] = [];

  if (!existsSync(dir)) {
    return results;
  }

  try {
    const entries = readdirSync(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = nodePath.join(dir, entry.name);

      if (entry.isDirectory()) {
        // Skip node_modules
        if (entry.name !== "node_modules") {
          results.push(...walkDirectory(fullPath, extensions));
        }
      } else if (entry.isFile()) {
        const ext = nodePath.extname(entry.name);
        if (extensions.includes(ext)) {
          results.push(fullPath);
        }
      }
    }
  } catch (e) {
    // Log error in case it is something the user can/should act on
    console.debug(`[moose] Failed to read directory ${dir}:`, e);
  }

  return results;
}

/**
 * Adds .js extension to relative import paths that don't have an extension.
 * Handles import/export from statements and dynamic imports.
 *
 * @param content - JavaScript file content
 * @param fileDir - Directory containing the file being processed (for resolving paths)
 * @returns Content with .js extensions added to relative imports
 */
function addJsExtensionToImports(content: string, fileDir?: string): string {
  // Pattern for 'from' statements with relative paths
  // Matches: from './foo', from "../bar", from './utils/helper'
  const fromPattern = /(from\s+['"])(\.\.?\/[^'"]*?)(['"])/g;

  // Pattern for side-effect-only imports with relative paths
  // Matches: import './foo', import "../bar" (no 'from' keyword, just importing for side effects)
  const bareImportPattern = /(import\s+['"])(\.\.?\/[^'"]*?)(['"])/g;

  // Pattern for dynamic imports with relative paths
  // Matches: import('./foo'), import("../bar")
  const dynamicPattern = /(import\s*\(\s*['"])(\.\.?\/[^'"]*?)(['"])/g;

  let result = content;

  // Process 'from' statements
  result = result.replace(fromPattern, (match, prefix, importPath, quote) => {
    return rewriteImportPath(match, prefix, importPath, quote, fileDir);
  });

  // Process side-effect-only imports
  result = result.replace(
    bareImportPattern,
    (match, prefix, importPath, quote) => {
      return rewriteImportPath(match, prefix, importPath, quote, fileDir);
    },
  );

  // Process dynamic imports
  result = result.replace(
    dynamicPattern,
    (match, prefix, importPath, quote) => {
      return rewriteImportPath(match, prefix, importPath, quote, fileDir);
    },
  );

  return result;
}

/**
 * Rewrites a single import path to add .js extension if needed.
 * Handles directory imports with index files correctly.
 *
 * @param match - The full matched string
 * @param prefix - The prefix (from ' or import(')
 * @param importPath - The import path
 * @param quote - The closing quote
 * @param fileDir - Directory containing the file being processed (for resolving paths)
 * @returns The rewritten import or original if no change needed
 */
function rewriteImportPath(
  match: string,
  prefix: string,
  importPath: string,
  quote: string,
  fileDir?: string,
): string {
  // Skip if already has a JavaScript extension
  if (/\.[cm]?js$/.test(importPath)) {
    return match;
  }

  // Skip if importing a JSON file
  if (/\.json$/.test(importPath)) {
    return match;
  }

  // If we have the file directory, check if the import target is a directory with index.js
  if (fileDir) {
    const resolvedPath = nodePath.resolve(fileDir, importPath);

    // Check if path.js exists (regular file import)
    if (existsSync(`${resolvedPath}.js`)) {
      return `${prefix}${importPath}.js${quote}`;
    }

    // Check if path/index.js exists (directory import)
    if (existsSync(nodePath.join(resolvedPath, "index.js"))) {
      // For directory imports, rewrite to include /index.js
      return `${prefix}${importPath}/index.js${quote}`;
    }

    // Check for .mjs variants
    if (existsSync(`${resolvedPath}.mjs`)) {
      return `${prefix}${importPath}.mjs${quote}`;
    }
    if (existsSync(nodePath.join(resolvedPath, "index.mjs"))) {
      return `${prefix}${importPath}/index.mjs${quote}`;
    }

    // Check for .cjs variants
    if (existsSync(`${resolvedPath}.cjs`)) {
      return `${prefix}${importPath}.cjs${quote}`;
    }
    if (existsSync(nodePath.join(resolvedPath, "index.cjs"))) {
      return `${prefix}${importPath}/index.cjs${quote}`;
    }
  }

  // Default: add .js extension (fallback when no fileDir or file not found)
  return `${prefix}${importPath}.js${quote}`;
}

/**
 * Rewrites relative import paths in JavaScript files to include .js extensions.
 * This is required for Node.js ESM which requires explicit extensions.
 *
 * Handles:
 * - import statements: import { foo } from './bar' -> import { foo } from './bar.js'
 * - dynamic imports: import('./bar') -> import('./bar.js')
 * - re-exports: export { foo } from './bar' -> export { foo } from './bar.js'
 *
 * Does NOT modify:
 * - Package imports (no leading . or ..)
 * - Imports that already have extensions
 * - Imports from node_modules
 *
 * @param outDir - Directory containing compiled JavaScript files
 */
export function rewriteImportExtensions(outDir: string): void {
  const files = walkDirectory(outDir, [".js", ".mjs"]);

  for (const filePath of files) {
    const content = readFileSync(filePath, "utf-8");
    const fileDir = nodePath.dirname(filePath);
    const rewritten = addJsExtensionToImports(content, fileDir);

    if (content !== rewritten) {
      writeFileSync(filePath, rewritten, "utf-8");
    }
  }
}

// Import types from commons-types to use in this module
import type { KafkaClientConfig, Logger } from "./commons-types";

/**
 * Creates the base producer configuration for Kafka.
 * Used by both the SDK stream publishing and streaming function workers.
 *
 * @param maxMessageBytes - Optional max message size in bytes (synced with topic config)
 * @returns Producer configuration object for the Confluent Kafka client
 */
export function createProducerConfig(maxMessageBytes?: number) {
  return {
    kafkaJS: {
      idempotent: false, // Not needed for at-least-once delivery
      acks: ACKs,
      retry: {
        retries: MAX_RETRIES_PRODUCER,
        maxRetryTime: MAX_RETRY_TIME_MS,
      },
    },
    "linger.ms": 0, // This is to make sure at least once delivery with immediate feedback on the send
    ...(maxMessageBytes && { "message.max.bytes": maxMessageBytes }),
  };
}

/**
 * Parses a comma-separated broker string into an array of valid broker addresses.
 * Handles whitespace trimming and filters out empty elements.
 *
 * @param brokerString - Comma-separated broker addresses (e.g., "broker1:9092, broker2:9092, , broker3:9092")
 * @returns Array of trimmed, non-empty broker addresses
 */
const parseBrokerString = (brokerString: string): string[] =>
  brokerString
    .split(",")
    .map((b) => b.trim())
    .filter((b) => b.length > 0);

/**
 * Dynamically creates and connects a KafkaJS producer using the provided configuration.
 * Returns a connected producer instance.
 *
 * @param cfg - Kafka client configuration
 * @param logger - Logger instance
 * @param maxMessageBytes - Optional max message size in bytes (synced with topic config)
 */
export async function getKafkaProducer(
  cfg: KafkaClientConfig,
  logger: Logger,
  maxMessageBytes?: number,
): Promise<Producer> {
  const kafka = await getKafkaClient(cfg, logger);

  const producer = kafka.producer(createProducerConfig(maxMessageBytes));
  await producer.connect();
  return producer;
}

/**
 * Builds SASL configuration for Kafka client authentication
 */
const buildSaslConfig = (
  logger: Logger,
  args: KafkaClientConfig,
): SASLOptions | undefined => {
  const mechanism = args.saslMechanism ? args.saslMechanism.toLowerCase() : "";
  switch (mechanism) {
    case "plain":
    case "scram-sha-256":
    case "scram-sha-512":
      return {
        mechanism: mechanism,
        username: args.saslUsername || "",
        password: args.saslPassword || "",
      };
    default:
      logger.warn(`Unsupported SASL mechanism: ${args.saslMechanism}`);
      return undefined;
  }
};

/**
 * Dynamically creates a KafkaJS client configured with provided settings.
 * Use this to construct producers/consumers with custom options.
 */
export const getKafkaClient = async (
  cfg: KafkaClientConfig,
  logger: Logger,
): Promise<Kafka> => {
  const brokers = parseBrokerString(cfg.broker || "");
  if (brokers.length === 0) {
    throw new Error(`No valid broker addresses found in: "${cfg.broker}"`);
  }

  logger.log(`Creating Kafka client with brokers: ${brokers.join(", ")}`);
  logger.log(`Security protocol: ${cfg.securityProtocol || "plaintext"}`);
  logger.log(`Client ID: ${cfg.clientId}`);

  const saslConfig = buildSaslConfig(logger, cfg);

  return new Kafka({
    kafkaJS: {
      clientId: cfg.clientId,
      brokers,
      ssl: cfg.securityProtocol === "SASL_SSL",
      ...(saslConfig && { sasl: saslConfig }),
      retry: {
        initialRetryTime: RETRY_INITIAL_TIME_MS,
        maxRetryTime: MAX_RETRY_TIME_MS,
        retries: MAX_RETRIES,
      },
    },
  });
};
