import type { Plugin } from "esbuild";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "fs";
import { resolve, basename, join } from "path";
import { defineConfig, type Options } from "tsup";

/**
 * esbuild plugin that replaces native/heavy dependencies with empty stubs.
 *
 * When ts-moose-lib code is inlined, some transitive imports to native modules
 * (e.g. @514labs/kafka-javascript) end up as top-level require() calls in CJS.
 * These crash in Lambda because the native modules aren't installed.
 *
 * This plugin intercepts those imports and replaces them with an empty module,
 * so the bundle loads cleanly. Any code that actually tries to USE the native
 * functionality will get undefined — but serverless users don't call those paths.
 */
const stubNativeModules: Plugin = {
  name: "stub-native-modules",
  setup(build) {
    const nativeModules = [
      "@514labs/kafka-javascript",
      "@kafkajs/confluent-schema-registry",
      "@temporalio/activity",
      "@temporalio/client",
      "@temporalio/common",
      "@temporalio/worker",
      "@temporalio/workflow",
      "redis",
    ];

    const filter = new RegExp(
      `^(${nativeModules.map((m) => m.replace("/", "\\/")).join("|")})$`,
    );

    build.onResolve({ filter }, (args) => ({
      path: args.path,
      namespace: "stub-native",
    }));

    build.onLoad({ filter: /.*/, namespace: "stub-native" }, () => ({
      contents: `
        // Deep proxy that returns itself for any property access or function call.
        // This prevents crashes when bundled code destructures nested properties
        // (e.g. const { Kafka } = KafkaJS) or calls constructors.
        //
        // esbuild's __toESM helper does:
        //   target = Object.create(Object.getPrototypeOf(mod))
        //   __copyProps(target, mod)
        //
        // By making getPrototypeOf return the proxy itself, Object.create(proxy)
        // produces an object whose prototype IS the proxy, so any property access
        // (like .KafkaJS) falls through to the proxy's get trap and returns proxy.
        function createDeepProxy() {
          var handler = {
            get: function(_, prop) {
              if (prop === '__esModule') return true;
              if (prop === 'default') return proxy;
              if (typeof prop === 'symbol') return undefined;
              return proxy;
            },
            apply: function() { return proxy; },
            construct: function() { return proxy; },
            ownKeys: function() { return ['length', 'name', 'prototype']; },
            getOwnPropertyDescriptor: function(target, prop) {
              if (prop === 'length' || prop === 'name' || prop === 'prototype') {
                return Object.getOwnPropertyDescriptor(target, prop);
              }
              return undefined;
            },
            getPrototypeOf: function() { return proxy; },
          };
          var proxy = new Proxy(function(){}, handler);
          return proxy;
        }
        module.exports = createDeepProxy();
      `,
      loader: "js",
    }));
  },
};

/**
 * esbuild plugin that loads the upstream compiler plugin and patches isMooseFile()
 * to also recognize @bayoudhi/moose-lib-serverless import paths.
 *
 * The upstream compiler plugin only transforms `new OlapTable<T>(...)` when the
 * constructor's declaration file is in a path containing "@514labs/moose-lib".
 * This patch adds "@bayoudhi/moose-lib-serverless" to that check.
 */
const patchedCompilerPlugin: Plugin = {
  name: "patched-compiler-plugin",
  setup(build) {
    // Intercept our stub compilerPlugin.ts and replace it with the patched upstream
    build.onLoad(
      { filter: /moose-lib-serverless\/src\/compilerPlugin\.ts$/ },
      () => {
        // Read the upstream compiled compiler plugin
        const upstreamPath = resolve(
          __dirname,
          "node_modules/@514labs/moose-lib/dist/compilerPlugin.js",
        );
        let contents = readFileSync(upstreamPath, "utf8");

        // Patch isMooseFile to recognize our package
        contents = contents.replace(
          'location.includes("@514labs/moose-lib")',
          'location.includes("@514labs/moose-lib") || location.includes("@bayoudhi/moose-lib-serverless")',
        );

        return { contents, loader: "js" };
      },
    );
  },
};

/**
 * esbuild plugin that loads the upstream moose-tspc binary and patches the
 * compiler plugin path to reference @bayoudhi/moose-lib-serverless.
 *
 * The upstream moose-tspc.js creates a temporary tsconfig with the Moose compiler
 * plugins and runs tspc. It hardcodes the plugin path as:
 *   ./node_modules/@514labs/moose-lib/dist/compilerPlugin.js
 *
 * This patch rewrites that to use our package's compiler plugin instead.
 */
const patchedMooseTspc: Plugin = {
  name: "patched-moose-tspc",
  setup(build) {
    build.onLoad(
      { filter: /moose-lib-serverless\/src\/moose-tspc\.ts$/ },
      () => {
        const upstreamPath = resolve(
          __dirname,
          "node_modules/@514labs/moose-lib/dist/moose-tspc.js",
        );
        let contents = readFileSync(upstreamPath, "utf8");

        // Strip the upstream shebang — tsup's banner config adds our own
        contents = contents.replace(/^#!.*\n/, "");

        // Patch the compiler plugin path to use require.resolve() so it works
        // in monorepos with hoisted node_modules (where ./node_modules/... fails)
        contents = contents.replace(
          '"./node_modules/@514labs/moose-lib/dist/compilerPlugin.js"',
          'require.resolve("@bayoudhi/moose-lib-serverless/dist/compilerPlugin.js")',
        );

        return { contents, loader: "js" };
      },
    );
  },
};
/**
 * esbuild plugin that loads the upstream moose-runner binary and patches:
 * 1. The compiler plugin path to reference @bayoudhi/moose-lib-serverless
 * 2. The version reported by `print-version` to match the upstream moose-lib version
 *
 * moose-runner is invoked by the moose CLI to serialize data models, run
 * consumption APIs, streaming functions, and scripts. The upstream version
 * hardcodes the compiler plugin path as:
 *   ./node_modules/@514labs/moose-lib/dist/compilerPlugin.js
 *
 * The CLI's version check (added in v0.6.417) runs `moose-runner print-version`
 * and compares the output to CLI_VERSION. Our moose-runner must report the
 * upstream version (not our package version) to pass this check.
 */
const patchedMooseRunner: Plugin = {
  name: "patched-moose-runner",
  setup(build) {
    build.onLoad(
      { filter: /moose-lib-serverless\/src\/moose-runner\.ts$/ },
      () => {
        const upstreamPath = resolve(
          __dirname,
          "node_modules/@514labs/moose-lib/dist/moose-runner.js",
        );
        let contents = readFileSync(upstreamPath, "utf8");

        // Read the upstream moose-lib version for the print-version command
        const upstreamPkgPath = resolve(
          __dirname,
          "node_modules/@514labs/moose-lib/package.json",
        );
        const upstreamVersion = JSON.parse(
          readFileSync(upstreamPkgPath, "utf8"),
        ).version;

        // Strip the upstream shebang — tsup's banner config adds our own
        contents = contents.replace(/^#!.*\n/, "");

        // Patch the compiler plugin path to use require.resolve() so it works
        // in monorepos with hoisted node_modules (where ./node_modules/... fails)
        contents = contents.replace(
          '"./node_modules/@514labs/moose-lib/dist/compilerPlugin.js"',
          'require.resolve("@bayoudhi/moose-lib-serverless/dist/compilerPlugin.js")',
        );

        // Patch the version reading to return the upstream moose-lib version
        // instead of reading our package.json (which has a different version).
        // The CLI compares this output to its own version (CLI_VERSION).
        contents = contents.replace(
          /var packageJson = JSON\.parse\(\n\s*\(0, import_fs2\.readFileSync\)\(\(0, import_path2\.join\)\(__dirname, "\.\.", "package\.json"\), "utf-8"\)\n\);/,
          `var packageJson = { version: "${upstreamVersion}" };`,
        );

        return { contents, loader: "js" };
      },
    );
  },
};

/**
 * Copy upstream .d.ts files into our dist/ and create self-contained type
 * declarations. This replaces tsup's broken DTS generation which fails to
 * inline chunked type files from the upstream npm package.
 *
 * The upstream @514labs/moose-lib/dist/ has chunked types:
 *   index.d.ts          → re-exports from ./index-DdE-_e4q.js and ./browserCompatible.js
 *   index-DdE-_e4q.d.ts → main chunk with all type declarations (OlapTable, Stream, etc.)
 *   browserCompatible.d.ts → subset re-exporting Key, JWT from the chunk
 *
 * We copy the chunk files as-is, then create our own index.d.ts that:
 * 1. Rewrites .js references to .d.ts so TypeScript resolves them locally
 * 2. Removes the @514labs/kafka-javascript import (native dep not installed)
 * 3. Stubs the Kafka and Producer types that reference that import
 * 4. Appends our configureClickHouse() and ClickHouseConfig declarations
 *
 * This ensures the compiler plugin's isMooseFile() check succeeds because
 * all .d.ts files live under @bayoudhi/moose-lib-serverless/dist/.
 */
function copyUpstreamTypes(): void {
  const upstreamDist = resolve(
    __dirname,
    "node_modules/@514labs/moose-lib/dist",
  );
  const outDir = resolve(__dirname, "dist");

  if (!existsSync(outDir)) {
    mkdirSync(outDir, { recursive: true });
  }

  // 1. Copy all chunk .d.ts files (everything except index.d.ts which we rewrite)
  const chunkFiles = readdirSync(upstreamDist).filter(
    (f) =>
      f.endsWith(".d.ts") &&
      f !== "index.d.ts" &&
      f !== "compilerPlugin.d.ts" &&
      f !== "moose-runner.d.ts" &&
      f !== "moose-tspc.d.ts",
  );

  for (const chunk of chunkFiles) {
    // Read-patch-write instead of raw copy:
    // 1. Strip .js from relative imports so TS resolves to the .d.ts files
    //    (e.g. './index-DdE-_e4q.js' → './index-DdE-_e4q')
    // 2. Stub unavailable native module imports (@temporalio/client)
    let chunkContent = readFileSync(join(upstreamDist, chunk), "utf8");
    chunkContent = chunkContent.replace(/'\.\/(.*?)\.js'/g, "'./$1'");
    // Replace `import { Client } from '@temporalio/client'` with a local stub.
    // The Client type is used in WorkflowClient and getTemporalClient() which
    // serverless users don't call, but the declaration must be valid TypeScript.
    chunkContent = chunkContent.replace(
      /^import \{ Client \} from '@temporalio\/client';$/m,
      "type Client = any;",
    );
    // Also remove bare `import '@temporalio/client'` if present
    chunkContent = chunkContent.replace(
      /^import '@temporalio\/client';\n?/m,
      "",
    );
    writeFileSync(join(outDir, chunk), chunkContent, "utf8");
  }

  // 2. Read upstream index.d.ts and patch it
  let indexDts = readFileSync(join(upstreamDist, "index.d.ts"), "utf8");

  // Rewrite relative .js references to bare specifiers so TypeScript resolves
  // them to the .d.ts files in the same directory.
  // e.g. './index-DdE-_e4q.js' → './index-DdE-_e4q'
  //      './browserCompatible.js' → './browserCompatible'
  indexDts = indexDts.replace(/'\.\/(.*?)\.js'/g, "'./$1'");

  // Remove unavailable native module imports
  indexDts = indexDts.replace(
    /^import \{ KafkaJS \} from '@514labs\/kafka-javascript';\n/m,
    "",
  );
  indexDts = indexDts.replace(/^import '@temporalio\/client';\n?/m, "");

  // Stub the Kafka and Producer types that referenced the removed import.
  // The upstream declares:
  //   declare const Kafka: typeof KafkaJS.Kafka;
  //   type Kafka = KafkaJS.Kafka;
  //   type Producer = KafkaJS.Producer;
  // We replace with `any` stubs since serverless users don't use Kafka.
  indexDts = indexDts.replace(
    /^declare const Kafka:.*$/m,
    "declare const Kafka: any;",
  );
  indexDts = indexDts.replace(/^type Kafka = .*$/m, "type Kafka = any;");
  indexDts = indexDts.replace(/^type Producer = .*$/m, "type Producer = any;");

  // 3. Append our serverless-specific declarations
  const serverlessDeclarations = `
// ── @bayoudhi/moose-lib-serverless additions ──────────────────────────────

/**
 * ClickHouse connection configuration for serverless environments.
 *
 * In a standard Moose project, connection details are read from
 * \`moose.config.toml\`. In serverless environments (AWS Lambda, Edge, etc.)
 * this file doesn't exist, so you must provide the config programmatically
 * via {@link configureClickHouse} before calling \`.insert()\` on any table.
 */
export interface ClickHouseConfig {
  /** ClickHouse host (e.g. \"clickhouse.example.com\") */
  host: string;
  /** ClickHouse HTTP port as a string (e.g. \"8443\") */
  port: string;
  /** ClickHouse username (e.g. \"default\") */
  username: string;
  /** ClickHouse password */
  password: string;
  /** ClickHouse database name (optional — only needed if your OlapTable configs don't specify \`database\`) */
  database?: string;
  /** Whether to use HTTPS/SSL for the connection */
  useSSL: boolean;
}

/**
 * Configure the ClickHouse connection for serverless environments.
 *
 * Call this once during cold start (before any \`.insert()\` calls) to provide
 * ClickHouse connection details. This bypasses the \`moose.config.toml\` file
 * lookup that would otherwise fail in environments without a Moose project
 * structure.
 */
export declare function configureClickHouse(config: ClickHouseConfig): void;
`;

  indexDts += serverlessDeclarations;

  // 4. Write both CJS (.d.ts) and ESM (.d.mts) type declarations
  writeFileSync(join(outDir, "index.d.ts"), indexDts, "utf8");
  writeFileSync(join(outDir, "index.d.mts"), indexDts, "utf8");

  // Log what was copied for build visibility
  const copiedFiles = [
    ...chunkFiles,
    "index.d.ts (patched)",
    "index.d.mts (patched)",
  ];
  console.log(
    `[copyUpstreamTypes] Copied ${copiedFiles.length} type declaration files:`,
  );
  copiedFiles.forEach((f) => console.log(`  - ${f}`));
}

// ─── Build Configurations ───────────────────────────────────────────────────

/**
 * Main library build: serverless-compatible SDK classes and utilities.
 */
const libraryConfig: Options = {
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  // DTS is handled by copyUpstreamTypes() in onSuccess — see below.
  // tsup's dts.resolve doesn't fully inline the chunked upstream types,
  // and the broken references prevent the compiler plugin from working.
  dts: false,
  outDir: "dist",
  splitting: false,
  sourcemap: true,
  clean: true,

  // Inline @514labs/moose-lib so there is no runtime dependency on it.
  noExternal: ["@514labs/moose-lib"],

  // Externalize runtime deps that consumers install separately.
  external: [
    "@clickhouse/client",
    "@clickhouse/client-web",
    "csv-parse",
    "jose",
    "toml",
    "typia",
    "typescript",

    // Node builtins
    "fs",
    "path",
    "process",
    "node:stream",
  ],

  // Stub out native modules so they never crash at load time.
  esbuildPlugins: [stubNativeModules],

  // After the JS build, copy upstream .d.ts files into dist and create our
  // own index.d.ts / index.d.mts wrappers. This produces self-contained type
  // declarations that don't require @514labs/moose-lib to be installed.
  async onSuccess() {
    copyUpstreamTypes();
  },
};

/**
 * Compiler plugin build: patched version of the Moose compiler plugin.
 *
 * This is loaded by ts-patch at compile time (not at Lambda runtime), so:
 * - CJS only (ts-patch uses require())
 * - No .d.ts needed (consumers reference it by file path in tsconfig.json)
 * - typescript, ts-patch, and typia are external (peer deps at compile time)
 * - All other deps from moose-lib are already inlined in the upstream bundle
 */
const compilerPluginConfig: Options = {
  entry: ["src/compilerPlugin.ts"],
  format: ["cjs"],
  dts: false,
  outDir: "dist",
  splitting: false,
  sourcemap: false,
  // Don't clean — the library build already cleaned the output dir.
  clean: false,

  // The upstream compiler plugin is already a self-contained bundle.
  // Our esbuild plugin loads and patches it directly, so we don't need
  // noExternal here. We just need to make sure we don't try to resolve
  // any of its external dependencies.
  external: [
    "typescript",
    "ts-patch",

    // Typia internals — the plugin calls these directly at compile time
    "typia",
    "typia/lib/programmers/*",
    "typia/lib/factories/*",
    "typia/lib/transformers/*",
    "typia/lib/schemas/*",
    "typia/lib/tags",

    // ClickHouse client — pulled in transitively via commons.ts but never
    // called by the compiler plugin. Mark external so the require() is
    // preserved but never resolves unless installed.
    "@clickhouse/client",
    "@clickhouse/client-web",

    // Other transitive deps from the upstream bundle
    "csv-parse",
    "jose",
    "toml",

    // Node builtins
    "fs",
    "path",
    "process",
    "node:fs",
    "node:stream",
  ],

  esbuildPlugins: [patchedCompilerPlugin, stubNativeModules],
};

/**
 * moose-tspc binary build: patched version of the Moose TypeScript compiler.
 *
 * This binary is invoked by the moose CLI (e.g. `moose generate migration`)
 * to compile TypeScript models with the Moose compiler plugins.
 *
 * - CJS only (it's a Node.js CLI script)
 * - No .d.ts needed (consumers invoke it as a binary, not import it)
 * - Native modules are stubbed (Kafka is pulled in via commons.ts but unused)
 */
const mooseTspcConfig: Options = {
  entry: ["src/moose-tspc.ts"],
  format: ["cjs"],
  dts: false,
  outDir: "dist",
  splitting: false,
  sourcemap: false,
  // Don't clean — the library build already cleaned the output dir.
  clean: false,

  external: [
    // Node builtins used by moose-tspc
    "child_process",
    "fs",
    "path",
    "process",
  ],

  esbuildPlugins: [patchedMooseTspc, stubNativeModules],

  // Preserve the shebang for CLI execution
  banner: {
    js: "#!/usr/bin/env node",
  },
};

/**
 * moose-runner binary build: patched version of the Moose runner.
 *
 * This binary is invoked by the moose CLI to serialize data models,
 * run consumption APIs, streaming functions, and scripts.
 *
 * - CJS only (it's a Node.js CLI script)
 * - No .d.ts needed (consumers invoke it as a binary, not import it)
 * - Native modules are stubbed (Kafka, Temporal, Redis are unused in CI)
 * - Runtime deps (commander, ts-node, etc.) are externalized
 */
const mooseRunnerConfig: Options = {
  entry: ["src/moose-runner.ts"],
  format: ["cjs"],
  dts: false,
  outDir: "dist",
  splitting: false,
  sourcemap: false,
  // Don't clean — the library build already cleaned the output dir.
  clean: false,

  external: [
    // Runtime deps that moose-runner needs (installed transitively)
    "@clickhouse/client",
    "@clickhouse/client-web",
    "commander",
    "csv-parse",
    "jose",
    "toml",
    "ts-node",
    "tsconfig-paths",
    "ts-patch",
    "typescript",

    // Node builtins
    "async_hooks",
    "buffer",
    "child_process",
    "cluster",
    "crypto",
    "fs",
    "http",
    "os",
    "path",
    "perf_hooks",
    "process",
    "stream",
    "util",
  ],

  esbuildPlugins: [patchedMooseRunner, stubNativeModules],

  // Preserve the shebang for CLI execution
  banner: {
    js: "#!/usr/bin/env node",
  },
};

export default defineConfig([
  libraryConfig,
  compilerPluginConfig,
  mooseTspcConfig,
  mooseRunnerConfig,
]);
