# Changelog

## v0.4.2 - Jun 30, 2026

Initial maintained release of `xkafka`.

### Added

* Unified **Kafka client** for producing and consuming.
* **Synchronous and asynchronous** producing modes.
* **Batch consumer** handlers with safe offset commits.
* Transactional producing via **`RunInTx`**.
* **`GroupTransactSession`** for Kafka-to-Kafka exactly-once semantics (EOS).
* Atomic commits of produced records and consumed offsets within a single transaction.
* Automatic transaction abort on handler errors or panics.
* **Share Group** consumption support with `AckAccept`, `AckRelease`, and `AckReject` behaviors.
* **TLS and SASL** configuration support.
* **OpenTelemetry** hooks for Kafka client metrics and tracing.
* **OpenTelemetry tracing** with backend-agnostic `TracerProvider` and `TextMapPropagator`.
* **Prometheus** wrapper-level metrics.
* Shared **`Config`** structure for producer, consumer, Share Group, and transaction workflows.
* Environment variable tags for **`Config`**.
* Runnable examples for producing, consuming, transactions, EOS, Share Groups, and env-based configuration.
* README documentation for core features, configuration, metrics, and examples.

### Notes

* **`GroupTransactSession`** is designed for consume-process-produce loops requiring atomic commits of records and
  offsets.