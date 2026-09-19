# Changelog

All notable changes to this project will be documented in this file.

## v0.7.0

### Fixed

* **Producer Transactions:** `RunInTx` now recovers transaction function panics, attempts to abort the active
  transaction, and returns the panic as an error instead of re-panicking. Panic values that implement `error` are
  preserved in the returned error chain.

### Changed

* **franz-go:** Updated the core dependency to `v1.22.0`.

---

## extra/otelxkafka/v0.7.0

### Changed

* **xkafka:** Updated the core dependency to `v0.7.0`.
* **franz-go:** Updated the dependency to `v1.22.0`.

---

## v0.6.0

Initial production release of `xkafka`, introducing a compact runtime layer built directly on top of `franz-go`.

### Added

* **Unified Kafka Client:** Single `Client` instance for synchronous, asynchronous, non-blocking, and transactional
  producing alongside handler-driven consumption.
* **Native franz-go Configuration:** Direct support for native `kgo.Opt` values without introducing a parallel
  configuration layer.
* **Batch Processing:** Unified batch handler model for regular consumer groups and Kafka Share Groups, with
  configurable retries, backoff, and panic recovery.
* **Kafka Share Groups (KIP-932):** Record-level accept, release, broker redelivery, delivery-count based rejection,
  configurable release delays, and acknowledgement flushing.
* **Producer Transactions:** Managed transactional workflows through `RunInTx` with automatic commit and abort handling,
  panic recovery, buffered-record cleanup, and lifecycle hooks.
* **Exactly-Once Semantics (EOS):** `GroupTransactSession` for Kafka-to-Kafka consume-process-produce workflows with
  atomic commits of produced records and consumed offsets.
* **Runtime Hooks:** Composable hooks for client lifecycle, producing, fetching, processing, offset commits, Share Group
  acknowledgements, and transactions.
* **Runnable Examples:** Examples covering basic workflows, transactions, Share Groups, exactly-once semantics, and
  OpenTelemetry instrumentation.

---

## extra/otelxkafka/v0.6.0

Initial release of the `otelxkafka` OpenTelemetry integration module.

### Added

* **Runtime Metrics:** OpenTelemetry counters and histograms for produce and fetch errors, handler processing, offset
  commits, Share Group acknowledgements, and transactions.
* **Distributed Tracing:** OpenTelemetry spans for synchronous producing, consumer batch processing, offset commits,
  Share Group acknowledgement flushes, and transactions.
* **Context Propagation:** Context injection and extraction through Kafka record headers, with consumer processing spans
  linked to propagated message creation contexts using OpenTelemetry span links.
* **Telemetry Attributes:** Configurable client IDs, consumer groups, Share Groups, and custom labels shared across
  metrics and traces.
* **Kotel:** Composition of meter and tracer instrumentation into standard `xkafka` runtime hooks.
* **franz-go Interoperability:** Support for using `otelxkafka` alongside `franz-go/plugin/kotel` when native franz-go
  client metrics are required.
