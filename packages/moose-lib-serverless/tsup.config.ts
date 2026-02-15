import type { Plugin } from "esbuild";
import { defineConfig } from "tsup";

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
      contents: "module.exports = {};",
      loader: "js",
    }));
  },
};

export default defineConfig({
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
});
