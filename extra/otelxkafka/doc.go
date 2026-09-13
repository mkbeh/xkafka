// Package otelxkafka provides OpenTelemetry metrics, tracing, and context
// propagation for github.com/mkbeh/xkafka.
//
// [Meter] records xkafka runtime metrics, while [Tracer] traces synchronous
// produce operations, message processing, offset commits, Share Group
// acknowledgement flushes, and transactions.
//
// Meter and Tracer can be registered directly with xkafka or combined using
// [Kotel].
package otelxkafka
