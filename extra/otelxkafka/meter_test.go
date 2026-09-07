package otelxkafka

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

func TestMeterErrorMetrics(t *testing.T) {
	meter, reader := newTestMeter(t)
	meter.setRuntime("orders", map[string]string{"service": "orders-api"}, "orders-group", "")

	meter.OnProduceError(
		&kgo.Record{Topic: "orders", Partition: 2},
		kerr.UnknownTopicOrPartition,
	)
	meter.OnFetchError(
		context.Background(),
		"payments",
		3,
		true,
		kerr.NotLeaderForPartition,
	)

	metrics := collectMetrics(t, reader)

	produce := int64Sum(t, metricByName(t, metrics, produceErrorsMetricName))
	if len(produce.DataPoints) != 1 {
		t.Fatalf("produce error data points = %d, want 1", len(produce.DataPoints))
	}
	producePoint := produce.DataPoints[0]
	if producePoint.Value != 1 {
		t.Fatalf("produce error value = %d, want 1", producePoint.Value)
	}
	assertSetStringAttribute(t, producePoint.Attributes, clientIDAttribute, "orders")
	assertSetStringAttribute(t, producePoint.Attributes, destinationNameAttribute, "orders")
	assertSetStringAttribute(t, producePoint.Attributes, destinationPartitionIDAttribute, "2")
	assertSetStringAttribute(t, producePoint.Attributes, errorTypeAttribute, kerr.UnknownTopicOrPartition.Message)

	fetch := int64Sum(t, metricByName(t, metrics, fetchErrorsMetricName))
	if len(fetch.DataPoints) != 1 {
		t.Fatalf("fetch error data points = %d, want 1", len(fetch.DataPoints))
	}
	fetchPoint := fetch.DataPoints[0]
	if fetchPoint.Value != 1 {
		t.Fatalf("fetch error value = %d, want 1", fetchPoint.Value)
	}
	assertSetStringAttribute(t, fetchPoint.Attributes, consumerGroupAttribute, "orders-group")
	assertSetStringAttribute(t, fetchPoint.Attributes, destinationNameAttribute, "payments")
	assertSetStringAttribute(t, fetchPoint.Attributes, destinationPartitionIDAttribute, "3")
	assertSetBoolAttribute(t, fetchPoint.Attributes, fetchErrorRecoverableAttribute, true)
	assertSetStringAttribute(t, fetchPoint.Attributes, errorTypeAttribute, kerr.NotLeaderForPartition.Message)
}

func TestMeterHandlerMetrics(t *testing.T) {
	meter, reader := newTestMeter(t)
	meter.setRuntime("consumer", nil, "consumer-group", "")

	records := []*kgo.Record{
		{Topic: "orders"},
		{Topic: "orders"},
		{Topic: "payments"},
	}

	meter.OnHandleEnd(context.Background(), records, 250*time.Millisecond, nil)
	meter.OnHandleEnd(
		context.Background(),
		records,
		500*time.Millisecond,
		kerr.UnknownTopicOrPartition,
	)

	metrics := collectMetrics(t, reader)

	process := float64Histogram(t, metricByName(t, metrics, "messaging.process.duration"))
	if len(process.DataPoints) != 2 {
		t.Fatalf("process duration data points = %d, want 2", len(process.DataPoints))
	}

	success := histogramPointWithoutAttribute(t, process.DataPoints, errorTypeAttribute)
	if success.Count != 1 || success.Sum != 0.25 {
		t.Fatalf("success process duration count/sum = %d/%v, want 1/0.25", success.Count, success.Sum)
	}
	assertSetStringAttribute(t, success.Attributes, consumerGroupAttribute, "consumer-group")
	assertSetStringAttribute(t, success.Attributes, operationNameAttribute, handleOperationName)
	assertSetStringAttribute(t, success.Attributes, messagingSystemAttribute, messagingSystemKafka)
	assertSetMissing(t, success.Attributes, destinationNameAttribute)
	if !reflect.DeepEqual(success.Bounds, messagingDurationBuckets) {
		t.Fatalf("process duration bounds = %v, want %v", success.Bounds, messagingDurationBuckets)
	}

	failed := histogramPointByStringAttribute(
		t,
		process.DataPoints,
		errorTypeAttribute,
		kerr.UnknownTopicOrPartition.Message,
	)
	if failed.Count != 1 || failed.Sum != 0.5 {
		t.Fatalf("failed process duration count/sum = %d/%v, want 1/0.5", failed.Count, failed.Sum)
	}
	assertSetMissing(t, failed.Attributes, destinationNameAttribute)

	handled := int64Sum(t, metricByName(t, metrics, handleRecordsMetricName))
	if len(handled.DataPoints) != 2 {
		t.Fatalf("handler record data points = %d, want 2", len(handled.DataPoints))
	}

	orders := int64PointByStringAttribute(t, handled.DataPoints, destinationNameAttribute, "orders")
	if orders.Value != 2 {
		t.Fatalf("orders handled records = %d, want 2", orders.Value)
	}
	assertSetStringAttribute(t, orders.Attributes, consumerGroupAttribute, "consumer-group")

	payments := int64PointByStringAttribute(t, handled.DataPoints, destinationNameAttribute, "payments")
	if payments.Value != 1 {
		t.Fatalf("payments handled records = %d, want 1", payments.Value)
	}
}

func TestMeterSettlementMetrics(t *testing.T) {
	meter, reader := newTestMeter(t)

	meter.setRuntime("consumer", nil, "consumer-group", "")
	meter.OnOffsetCommit(context.Background(), 100*time.Millisecond, nil)

	meter.setRuntime("share", nil, "", "share-group")
	meter.OnShareAckFlush(
		context.Background(),
		200*time.Millisecond,
		kerr.UnknownTopicOrPartition,
	)

	metrics := collectMetrics(t, reader)
	operations := float64Histogram(t, metricByName(t, metrics, "messaging.client.operation.duration"))
	if len(operations.DataPoints) != 2 {
		t.Fatalf("client operation data points = %d, want 2", len(operations.DataPoints))
	}

	commit := histogramPointByStringAttribute(
		t,
		operations.DataPoints,
		operationNameAttribute,
		offsetCommitOperationName,
	)
	if commit.Count != 1 || commit.Sum != 0.1 {
		t.Fatalf("commit duration count/sum = %d/%v, want 1/0.1", commit.Count, commit.Sum)
	}
	assertSetStringAttribute(t, commit.Attributes, consumerGroupAttribute, "consumer-group")
	assertSetStringAttribute(t, commit.Attributes, operationTypeAttribute, settleOperationType)
	assertSetStringAttribute(t, commit.Attributes, messagingSystemAttribute, messagingSystemKafka)
	assertSetMissing(t, commit.Attributes, errorTypeAttribute)
	if !reflect.DeepEqual(commit.Bounds, messagingDurationBuckets) {
		t.Fatalf("client operation bounds = %v, want %v", commit.Bounds, messagingDurationBuckets)
	}

	ack := histogramPointByStringAttribute(
		t,
		operations.DataPoints,
		operationNameAttribute,
		shareAckOperationName,
	)
	if ack.Count != 1 || ack.Sum != 0.2 {
		t.Fatalf("ack duration count/sum = %d/%v, want 1/0.2", ack.Count, ack.Sum)
	}
	assertSetStringAttribute(t, ack.Attributes, shareGroupAttribute, "share-group")
	assertSetMissing(t, ack.Attributes, consumerGroupAttribute)
	assertSetStringAttribute(t, ack.Attributes, errorTypeAttribute, kerr.UnknownTopicOrPartition.Message)
}

func TestMeterShareAckRecords(t *testing.T) {
	meter, reader := newTestMeter(t)
	meter.setRuntime("share", nil, "", "share-group")

	meter.OnShareAck(context.Background(), xkafka.ShareAckAccept, 3)
	meter.OnShareAck(context.Background(), xkafka.ShareAckRelease, 1)

	metrics := collectMetrics(t, reader)
	acks := int64Sum(t, metricByName(t, metrics, shareAckRecordsMetricName))
	if len(acks.DataPoints) != 2 {
		t.Fatalf("share ack data points = %d, want 2", len(acks.DataPoints))
	}

	accepted := int64PointByStringAttribute(t, acks.DataPoints, shareAckOutcomeAttribute, "accept")
	if accepted.Value != 3 {
		t.Fatalf("accepted records = %d, want 3", accepted.Value)
	}
	assertSetStringAttribute(t, accepted.Attributes, shareGroupAttribute, "share-group")
	assertSetMissing(t, accepted.Attributes, consumerGroupAttribute)

	released := int64PointByStringAttribute(t, acks.DataPoints, shareAckOutcomeAttribute, "release")
	if released.Value != 1 {
		t.Fatalf("released records = %d, want 1", released.Value)
	}
}

func TestMeterTransactionDuration(t *testing.T) {
	meter, reader := newTestMeter(t)

	meter.setRuntime("producer", nil, "", "")
	meter.OnTransactionEnd(
		context.Background(),
		xkafka.TransactionTypeProducer,
		xkafka.TransactionOutcomeCommit,
		500*time.Millisecond,
		nil,
	)

	meter.setRuntime("eos", nil, "eos-group", "")
	meter.OnTransactionEnd(
		context.Background(),
		xkafka.TransactionTypeGroup,
		xkafka.TransactionOutcomeAbort,
		750*time.Millisecond,
		kerr.UnknownTopicOrPartition,
	)

	metrics := collectMetrics(t, reader)
	transactions := float64Histogram(t, metricByName(t, metrics, transactionDurationMetricName))
	if len(transactions.DataPoints) != 2 {
		t.Fatalf("transaction duration data points = %d, want 2", len(transactions.DataPoints))
	}

	producer := histogramPointByStringAttribute(
		t,
		transactions.DataPoints,
		transactionTypeAttribute,
		string(xkafka.TransactionTypeProducer),
	)
	if producer.Count != 1 || producer.Sum != 0.5 {
		t.Fatalf("producer transaction count/sum = %d/%v, want 1/0.5", producer.Count, producer.Sum)
	}
	assertSetStringAttribute(
		t,
		producer.Attributes,
		transactionOutcomeAttribute,
		string(xkafka.TransactionOutcomeCommit),
	)
	assertSetMissing(t, producer.Attributes, consumerGroupAttribute)
	assertSetMissing(t, producer.Attributes, errorTypeAttribute)
	if !reflect.DeepEqual(producer.Bounds, transactionDurationBuckets) {
		t.Fatalf("transaction duration bounds = %v, want %v", producer.Bounds, transactionDurationBuckets)
	}

	group := histogramPointByStringAttribute(
		t,
		transactions.DataPoints,
		transactionTypeAttribute,
		string(xkafka.TransactionTypeGroup),
	)
	if group.Count != 1 || group.Sum != 0.75 {
		t.Fatalf("group transaction count/sum = %d/%v, want 1/0.75", group.Count, group.Sum)
	}
	assertSetStringAttribute(t, group.Attributes, consumerGroupAttribute, "eos-group")
	assertSetStringAttribute(
		t,
		group.Attributes,
		transactionOutcomeAttribute,
		string(xkafka.TransactionOutcomeAbort),
	)
	assertSetStringAttribute(t, group.Attributes, errorTypeAttribute, kerr.UnknownTopicOrPartition.Message)
}

func TestMeterInstrumentationScope(t *testing.T) {
	meter, reader := newTestMeter(t)
	meter.OnProduceError(&kgo.Record{Topic: "orders"}, kerr.UnknownTopicOrPartition)

	metrics := collectMetrics(t, reader)
	if len(metrics.ScopeMetrics) != 1 {
		t.Fatalf("scope metrics = %d, want 1", len(metrics.ScopeMetrics))
	}

	scope := metrics.ScopeMetrics[0].Scope
	if scope.Name != instrumentationName {
		t.Fatalf("scope name = %q, want %q", scope.Name, instrumentationName)
	}
	if scope.Version != semVersion() {
		t.Fatalf("scope version = %q, want %q", scope.Version, semVersion())
	}
	if scope.SchemaURL != semconv.SchemaURL {
		t.Fatalf("scope schema URL = %q, want %q", scope.SchemaURL, semconv.SchemaURL)
	}
}

func TestCountRecordsByTopic(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "orders"},
		nil,
		{Topic: "payments"},
		{Topic: "orders"},
		{Topic: ""},
	}

	got := countRecordsByTopic(records)
	want := map[string]int64{
		"orders":   2,
		"payments": 1,
		"":         1,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("topic counts = %#v, want %#v", got, want)
	}
}

func TestConsumerAttributesGroups(t *testing.T) {
	t.Run("consumer group", func(t *testing.T) {
		meter := &Meter{
			clientAttributes: attribute.NewSet(),
			consumerGroup:    "orders",
		}

		set := attribute.NewSet(meter.consumerAttributes()...)
		if value, ok := set.Value(attribute.Key(consumerGroupAttribute)); !ok || value.AsString() != "orders" {
			t.Fatalf("consumer group = %q, %v; want orders, true", value.AsString(), ok)
		}
		if _, ok := set.Value(attribute.Key(shareGroupAttribute)); ok {
			t.Fatal("share group attribute was set for regular consumer group")
		}
	})

	t.Run("share group", func(t *testing.T) {
		meter := &Meter{
			clientAttributes: attribute.NewSet(),
			shareGroup:       "workers",
		}

		set := attribute.NewSet(meter.consumerAttributes()...)
		if value, ok := set.Value(attribute.Key(shareGroupAttribute)); !ok || value.AsString() != "workers" {
			t.Fatalf("share group = %q, %v; want workers, true", value.AsString(), ok)
		}
		if _, ok := set.Value(attribute.Key(consumerGroupAttribute)); ok {
			t.Fatal("consumer group attribute was set for Share Group")
		}
	})
}

func newTestMeter(t *testing.T) (*Meter, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown meter provider: %v", err)
		}
	})

	return NewMeter(MeterProvider(provider)), reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	return metrics
}

func metricByName(t *testing.T, metrics metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric
			}
		}
	}

	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func int64Sum(t *testing.T, metric metricdata.Metrics) metricdata.Sum[int64] {
	t.Helper()

	data, ok := metric.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q data type = %T, want metricdata.Sum[int64]", metric.Name, metric.Data)
	}

	return data
}

func float64Histogram(t *testing.T, metric metricdata.Metrics) metricdata.Histogram[float64] {
	t.Helper()

	data, ok := metric.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q data type = %T, want metricdata.Histogram[float64]", metric.Name, metric.Data)
	}

	return data
}

func int64PointByStringAttribute(
	t *testing.T,
	points []metricdata.DataPoint[int64],
	key string,
	want string,
) metricdata.DataPoint[int64] {
	t.Helper()

	for _, point := range points {
		value, ok := point.Attributes.Value(attribute.Key(key))
		if ok && value.AsString() == want {
			return point
		}
	}

	t.Fatalf("int64 data point with %s=%q not found", key, want)
	return metricdata.DataPoint[int64]{}
}

func histogramPointByStringAttribute(
	t *testing.T,
	points []metricdata.HistogramDataPoint[float64],
	key string,
	want string,
) metricdata.HistogramDataPoint[float64] {
	t.Helper()

	for _, point := range points {
		value, ok := point.Attributes.Value(attribute.Key(key))
		if ok && value.AsString() == want {
			return point
		}
	}

	t.Fatalf("histogram data point with %s=%q not found", key, want)
	return metricdata.HistogramDataPoint[float64]{}
}

func histogramPointWithoutAttribute(
	t *testing.T,
	points []metricdata.HistogramDataPoint[float64],
	key string,
) metricdata.HistogramDataPoint[float64] {
	t.Helper()

	for _, point := range points {
		if _, ok := point.Attributes.Value(attribute.Key(key)); !ok {
			return point
		}
	}

	t.Fatalf("histogram data point without %q not found", key)
	return metricdata.HistogramDataPoint[float64]{}
}

func assertSetStringAttribute(t *testing.T, set attribute.Set, key string, want string) {
	t.Helper()

	value, ok := set.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %q not found", key)
	}
	if got := value.AsString(); got != want {
		t.Fatalf("attribute %q = %q, want %q", key, got, want)
	}
}

func assertSetBoolAttribute(t *testing.T, set attribute.Set, key string, want bool) {
	t.Helper()

	value, ok := set.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %q not found", key)
	}
	if got := value.AsBool(); got != want {
		t.Fatalf("attribute %q = %v, want %v", key, got, want)
	}
}

func assertSetMissing(t *testing.T, set attribute.Set, key string) {
	t.Helper()

	if _, ok := set.Value(attribute.Key(key)); ok {
		t.Fatalf("attribute %q unexpectedly found", key)
	}
}
