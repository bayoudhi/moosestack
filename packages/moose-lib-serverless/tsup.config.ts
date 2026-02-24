import type { Plugin } from "esbuild";
import { readFileSync } from "fs";
import { resolve } from "path";
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

        // Patch the compiler plugin path to use our package
        contents = contents.replace(
          "./node_modules/@514labs/moose-lib/dist/compilerPlugin.js",
          "./node_modules/@bayoudhi/moose-lib-serverless/dist/compilerPlugin.js",
        );

        return { contents, loader: "js" };
      },
    );
  },
};

// ─── Build Configurations ───────────────────────────────────────────────────

/**
 * Main library build: serverless-compatible SDK classes and utilities.
 */
const libraryConfig: Options = {
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  dts: true,
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

export default defineConfig([
  libraryConfig,
  compilerPluginConfig,
  mooseTspcConfig,
]);
