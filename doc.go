// Package xkafka provides Kafka producing, consuming, Share Group processing,
// and transactions on top of franz-go.
//
// Use [Client] for regular producer and consumer workloads and
// [GroupTransactSession] for Kafka-to-Kafka exactly-once
// consume-process-produce workflows. Native franz-go configuration can be
// supplied with [WithKafkaOptions].
package xkafka
