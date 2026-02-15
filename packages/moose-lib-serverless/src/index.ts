/**
 * @module @bayoudhi/moose-lib-serverless
 *
 * Serverless-compatible subset of @514labs/moose-lib.
 *
 * Re-exports everything from the `@514labs/moose-lib/serverless` entry point,
 * which excludes native C++ dependencies (Kafka, Temporal, Redis) that are
 * incompatible with AWS Lambda and edge runtimes.
 *
 * At build time, `@514labs/moose-lib` code is inlined into this package's
 * bundle so there is no runtime dependency on `@514labs/moose-lib`.
 */
export * from "@514labs/moose-lib/serverless";

/**
 * ClickHouse connection configuration for serverless environments.
 *
 * In a standard Moose project, connection details are read from
 * `moose.config.toml`. In serverless environments (AWS Lambda, Edge, etc.)
 * this file doesn't exist, so you must provide the config programmatically
 * via {@link configureClickHouse} before calling `.insert()` on any table.
 */
export interface ClickHouseConfig {
  /** ClickHouse host (e.g. "clickhouse.example.com") */
  host: string;
  /** ClickHouse HTTP port as a string (e.g. "8443") */
  port: string;
  /** ClickHouse username (e.g. "default") */
  username: string;
  /** ClickHouse password */
  password: string;
  /** ClickHouse database name */
  database: string;
  /** Whether to use HTTPS/SSL for the connection */
  useSSL: boolean;
}

// Internal: stores the ClickHouse config set by configureClickHouse() so it
// can be transferred to the real ConfigurationRegistry when init_runtime() runs.
let _pendingClickHouseConfig: ClickHouseConfig | null = null;

/**
 * Configure the ClickHouse connection for serverless environments.
 *
 * Call this once during cold start (before any `.insert()` calls) to provide
 * ClickHouse connection details. This bypasses the `moose.config.toml` file
 * lookup that would otherwise fail in environments without a Moose project
 * structure.
 *
 * @example
 * ```typescript
 * import { configureClickHouse, OlapTable } from "@bayoudhi/moose-lib-serverless";
 *
 * configureClickHouse({
 *   host: process.env.CLICKHOUSE_HOST!,
 *   port: process.env.CLICKHOUSE_PORT!,
 *   username: process.env.CLICKHOUSE_USER!,
 *   password: process.env.CLICKHOUSE_PASSWORD!,
 *   database: process.env.CLICKHOUSE_DATABASE!,
 *   useSSL: true,
 * });
 *
 * // Now .insert() works without moose.config.toml
 * await myTable.insert(data);
 * ```
 */
export function configureClickHouse(config: ClickHouseConfig): void {
  // The ConfigurationRegistry singleton lives on globalThis._mooseConfigRegistry.
  // It's initialized lazily inside an esbuild __esm block (init_runtime) that
  // only runs when OlapTable.insert() triggers `await import("config/runtime")`.
  //
  // configureClickHouse() may be called BEFORE any insert(), so the registry
  // may not exist yet. When init_runtime() later runs, it unconditionally does:
  //   globalThis._mooseConfigRegistry = ConfigurationRegistry.getInstance()
  // which would overwrite any shim we place there.
  //
  // Solution: use Object.defineProperty with a setter trap on globalThis.
  // When init_runtime() assigns the real registry, our setter intercept
  // transfers the pending config to it automatically.

  const registry = (globalThis as any)._mooseConfigRegistry;

  if (registry && typeof registry.setClickHouseConfig === "function") {
    // Registry already exists (init_runtime has run) — set directly
    registry.setClickHouseConfig(config);
    _pendingClickHouseConfig = null;
    return;
  }

  // Registry doesn't exist yet — store the config and set up an intercept
  _pendingClickHouseConfig = config;

  // Define a setter on globalThis._mooseConfigRegistry that will transfer
  // the pending config when init_runtime() assigns the real registry.
  let _realRegistry: any = null;
  Object.defineProperty(globalThis, "_mooseConfigRegistry", {
    configurable: true,
    enumerable: true,
    get() {
      return _realRegistry;
    },
    set(newRegistry: any) {
      _realRegistry = newRegistry;
      // Transfer the pending config to the real registry
      if (
        _pendingClickHouseConfig &&
        typeof newRegistry.setClickHouseConfig === "function"
      ) {
        newRegistry.setClickHouseConfig(_pendingClickHouseConfig);
        _pendingClickHouseConfig = null;
      }
      // Replace the property with a plain value to avoid future overhead
      Object.defineProperty(globalThis, "_mooseConfigRegistry", {
        configurable: true,
        enumerable: true,
        writable: true,
        value: newRegistry,
      });
    },
  });
}
