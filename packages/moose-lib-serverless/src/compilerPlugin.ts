/**
 * Compiler plugin entry point for @bayoudhi/moose-lib-serverless.
 *
 * This file serves as a marker for the build system. The actual compiler plugin
 * code is loaded from @514labs/moose-lib's compiled output and patched at build
 * time by the `patchIsMooseFile` esbuild plugin in tsup.config.ts.
 *
 * The patch modifies the `isMooseFile()` check to also recognize imports from
 * `@bayoudhi/moose-lib-serverless`, enabling the compiler plugin to transform
 * `new OlapTable<T>(...)` expressions that import from this package.
 *
 * Usage in tsconfig.json:
 *   {
 *     "compilerOptions": {
 *       "plugins": [
 *         { "transform": "./node_modules/@bayoudhi/moose-lib-serverless/dist/compilerPlugin.js" },
 *         { "transform": "typia/lib/transform" }
 *       ]
 *     }
 *   }
 */

// This export is replaced at build time by the patchedCompilerPlugin esbuild plugin.
// It exists only to satisfy TypeScript and provide a valid entry point for tsup.
const plugin = {};
export default plugin;
