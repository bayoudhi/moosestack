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
