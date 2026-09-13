package otelxkafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

func TestMeterHandleErrorAndRetry(t *testing.T) {
	t.Parallel()

	m, reader := newTestMeter(
		t,
		ClientID("client"),
		ConsumerGroup("workers"),
		Labels(map[string]string{"env": "test"}),
	)

	mixed := []*kgo.Record{
		{Topic: "a", Partition: 0},
		{Topic: "a", Partition: 1},
		{Topic: "b", Partition: 0},
	}
	retry := []*kgo.Record{{Topic: "a", Partition: 0}}
	handlerErr := errors.New("handler failed")

	m.OnHandleEnd(t.Context(), mixed, 40*time.Millisecond, nil)
	m.OnHandleEnd(t.Context(), retry, time.Millisecond, handlerErr)
	m.OnHandleEnd(t.Context(), retry, 10*time.Millisecond, nil)

	metrics := collectMetrics(t, reader)
	process := durationHistogram(t, metrics["messaging.process.duration"])
	if len(process.DataPoints) != 3 {
		t.Fatalf("process series = %d, want 3", len(process.DataPoints))
	}

	baseAttrs := []attribute.KeyValue{
		semconv.MessagingClientID("client"),
		semconv.MessagingConsumerGroupName("workers"),
		attribute.String("env", "test"),
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationName("process"),
	}

	for _, point := range process.DataPoints {
		wantAttrs := slices.Clone(baseAttrs)
		var wantDuration float64

		switch {
		case point.Attributes.HasValue(semconv.ErrorTypeKey):
			wantAttrs = append(
				wantAttrs,
				semconv.MessagingDestinationName("a"),
				semconv.ErrorTypeKey.String(testGenericErrorType),
			)
			wantDuration = 0.001
		case point.Attributes.HasValue(semconv.MessagingDestinationNameKey):
			wantAttrs = append(wantAttrs, semconv.MessagingDestinationName("a"))
			wantDuration = 0.01
		default:
			wantDuration = 0.04
		}

		assertAttributes(t, point.Attributes.ToSlice(), wantAttrs...)
		assertHistogramPoint(t, point, 1, wantDuration)
	}

	handled := sumData(t, metrics["xkafka.handler.records"])
	if len(handled.DataPoints) != 2 {
		t.Fatalf("record series = %d, want 2", len(handled.DataPoints))
	}

	wantRecords := map[string]int64{
		"a": 3,
		"b": 1,
	}
	for _, point := range handled.DataPoints {
		topic, ok := point.Attributes.Value(semconv.MessagingDestinationNameKey)
		if !ok {
			t.Fatal("handler record series has no destination")
		}

		topicName := topic.AsString()
		want, ok := wantRecords[topicName]
		if !ok {
			t.Fatalf("unexpected handler record destination %q", topicName)
		}
		if point.Value != want {
			t.Fatalf("successful records for %q = %d, want %d", topicName, point.Value, want)
		}

		assertAttributes(
			t,
			point.Attributes.ToSlice(),
			semconv.MessagingClientID("client"),
			semconv.MessagingConsumerGroupName("workers"),
			attribute.String("env", "test"),
			semconv.MessagingDestinationName(topicName),
		)
	}
}

func TestMeterErrors(t *testing.T) {
	t.Parallel()

	m, reader := newTestMeter(t, ClientID("client"), ConsumerGroup("workers"))
	m.OnProduceError(
		&kgo.Record{Topic: "a", Partition: 0, Context: t.Context()},
		fmt.Errorf("produce: %w", kerr.RequestTimedOut),
	)
	m.OnFetchError(t.Context(), "b", 2, true, kerr.UnknownTopicOrPartition)

	metrics := collectMetrics(t, reader)

	produceMetric := metrics["xkafka.produce.errors"]
	if produceMetric.Unit != "{record}" {
		t.Fatalf("produce error unit = %q, want {record}", produceMetric.Unit)
	}
	produced := onlySumPoint(t, sumData(t, produceMetric))
	if produced.Value != 1 {
		t.Fatalf("produce errors = %d, want 1", produced.Value)
	}
	assertAttributes(
		t,
		produced.Attributes.ToSlice(),
		semconv.MessagingClientID("client"),
		semconv.MessagingDestinationName("a"),
		semconv.ErrorTypeKey.String("REQUEST_TIMED_OUT"),
	)
	if produced.Attributes.HasValue(semconv.MessagingDestinationPartitionIDKey) {
		t.Fatal("producer error must not report an unassigned partition")
	}

	fetched := onlySumPoint(t, sumData(t, metrics["xkafka.fetch.errors"]))
	if fetched.Value != 1 {
		t.Fatalf("fetch errors = %d, want 1", fetched.Value)
	}
	assertAttributes(
		t,
		fetched.Attributes.ToSlice(),
		semconv.MessagingClientID("client"),
		semconv.MessagingConsumerGroupName("workers"),
		semconv.MessagingDestinationName("b"),
		semconv.MessagingDestinationPartitionID("2"),
		attribute.Bool("xkafka.fetch.error.recoverable", true),
		semconv.ErrorTypeKey.String("UNKNOWN_TOPIC_OR_PARTITION"),
	)
}

func TestMeterSettlement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		group         ClientOpt
		groupAttr     attribute.KeyValue
		record        func(*Meter, context.Context, time.Duration, error)
		duration      time.Duration
		err           error
		wantErrorType string
	}{
		{
			name:      "commit",
			group:     ConsumerGroup("workers"),
			groupAttr: semconv.MessagingConsumerGroupName("workers"),
			record:    (*Meter).OnOffsetCommit,
			duration:  5 * time.Millisecond,
		},
		{
			name:          "ack",
			group:         ShareGroup("shared"),
			groupAttr:     attribute.String("xkafka.share.group.name", "shared"),
			record:        (*Meter).OnShareAckFlush,
			duration:      20 * time.Millisecond,
			err:           kerr.RequestTimedOut,
			wantErrorType: "REQUEST_TIMED_OUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMeter(t, ClientID("client"), tt.group)
			tt.record(m, t.Context(), tt.duration, tt.err)

			point := onlyHistogramPoint(
				t,
				durationHistogram(t, collectMetrics(t, reader)["messaging.client.operation.duration"]),
			)
			wantAttrs := []attribute.KeyValue{
				semconv.MessagingClientID("client"),
				tt.groupAttr,
				semconv.MessagingSystemKafka,
				semconv.MessagingOperationName(tt.name),
				semconv.MessagingOperationTypeSettle,
			}
			if tt.wantErrorType != "" {
				wantAttrs = append(wantAttrs, semconv.ErrorTypeKey.String(tt.wantErrorType))
			}

			assertAttributes(t, point.Attributes.ToSlice(), wantAttrs...)
			assertHistogramPoint(t, point, 1, tt.duration.Seconds())
		})
	}
}

func TestMeterShareAck(t *testing.T) {
	t.Parallel()

	m, reader := newTestMeter(t, ShareGroup("shared"))
	tests := []struct {
		outcome xkafka.ShareAckOutcome
		count   int
	}{
		{outcome: xkafka.ShareAckAccept, count: 5},
		{outcome: xkafka.ShareAckRelease, count: 3},
		{outcome: xkafka.ShareAckReject, count: 2},
	}

	wantCounts := make(map[xkafka.ShareAckOutcome]int64, len(tests))
	for _, tt := range tests {
		m.OnShareAck(t.Context(), tt.outcome, tt.count)
		wantCounts[tt.outcome] = int64(tt.count)
	}

	data := sumData(t, collectMetrics(t, reader)["xkafka.share.ack.records"])
	if len(data.DataPoints) != len(tests) {
		t.Fatalf("ack series = %d, want %d", len(data.DataPoints), len(tests))
	}
	for _, point := range data.DataPoints {
		value, ok := point.Attributes.Value(attribute.Key("xkafka.share.ack.outcome"))
		if !ok {
			t.Fatal("share ack series has no outcome")
		}

		outcome := xkafka.ShareAckOutcome(value.AsString())
		want, ok := wantCounts[outcome]
		if !ok {
			t.Fatalf("unexpected share ack outcome %q", outcome)
		}
		if point.Value != want {
			t.Fatalf("ack count for %q = %d, want %d", outcome, point.Value, want)
		}

		assertAttributes(
			t,
			point.Attributes.ToSlice(),
			attribute.String("xkafka.share.group.name", "shared"),
			attribute.String("xkafka.share.ack.outcome", string(outcome)),
		)
	}
}

func TestMeterTransactions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		transactionType   xkafka.TransactionType
		outcome           xkafka.TransactionOutcome
		err               error
		wantType          string
		wantOutcome       string
		wantConsumerGroup bool
		wantErrorType     string
	}{
		{
			name:            "producer commit",
			transactionType: xkafka.TransactionTypeProducer,
			outcome:         xkafka.TransactionOutcomeCommit,
			wantType:        "producer",
			wantOutcome:     "commit",
		},
		{
			name:              "group error",
			transactionType:   xkafka.TransactionTypeGroup,
			outcome:           xkafka.TransactionOutcomeError,
			err:               kerr.RequestTimedOut,
			wantType:          "group",
			wantOutcome:       "error",
			wantConsumerGroup: true,
			wantErrorType:     "REQUEST_TIMED_OUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reader := newTestMeter(t, ClientID("client"), ConsumerGroup("workers"))
			m.OnTransactionEnd(t.Context(), tt.transactionType, tt.outcome, 50*time.Millisecond, tt.err)

			point := onlyHistogramPoint(
				t,
				durationHistogram(t, collectMetrics(t, reader)["xkafka.transaction.duration"]),
			)
			wantAttrs := []attribute.KeyValue{
				semconv.MessagingClientID("client"),
			}
			if tt.wantConsumerGroup {
				wantAttrs = append(wantAttrs, semconv.MessagingConsumerGroupName("workers"))
			}
			wantAttrs = append(
				wantAttrs,
				attribute.String("xkafka.transaction.type", tt.wantType),
				attribute.String("xkafka.transaction.outcome", tt.wantOutcome),
			)
			if tt.wantErrorType != "" {
				wantAttrs = append(wantAttrs, semconv.ErrorTypeKey.String(tt.wantErrorType))
			}

			assertAttributes(t, point.Attributes.ToSlice(), wantAttrs...)
			assertHistogramPoint(t, point, 1, 0.05)
		})
	}
}

func TestMeterHandleInstrumentsDisabledIndependently(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		disabled string
		enabled  string
	}{
		{
			name:     "process disabled",
			disabled: "messaging.process.duration",
			enabled:  "xkafka.handler.records",
		},
		{
			name:     "handler records disabled",
			disabled: "xkafka.handler.records",
			enabled:  "messaging.process.duration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(reader),
				sdkmetric.WithView(sdkmetric.NewView(
					sdkmetric.Instrument{Name: tt.disabled},
					sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}},
				)),
			)
			t.Cleanup(func() {
				if err := provider.Shutdown(context.WithoutCancel(t.Context())); err != nil {
					t.Error(err)
				}
			})

			m := NewMeter(MeterProvider(provider))
			m.OnHandleEnd(t.Context(), []*kgo.Record{{Topic: "a"}}, time.Millisecond, nil)

			metrics := collectMetrics(t, reader)
			if _, ok := metrics[tt.disabled]; ok {
				t.Fatalf("disabled metric %q was recorded", tt.disabled)
			}
			if _, ok := metrics[tt.enabled]; !ok {
				t.Fatalf("enabled metric %q was not recorded", tt.enabled)
			}
		})
	}
}

func TestMeterDurationBuckets(t *testing.T) {
	t.Parallel()

	m, reader := newTestMeter(t, ConsumerGroup("workers"))
	m.OnHandleEnd(t.Context(), []*kgo.Record{{Topic: "a"}}, time.Millisecond, nil)
	m.OnOffsetCommit(t.Context(), time.Millisecond, nil)
	m.OnTransactionEnd(
		t.Context(),
		xkafka.TransactionTypeProducer,
		xkafka.TransactionOutcomeCommit,
		time.Millisecond,
		nil,
	)

	want := []float64{
		0.001,
		0.005,
		0.01,
		0.025,
		0.05,
		0.1,
		0.25,
		0.5,
		1,
		2.5,
		5,
		10,
	}
	metrics := collectMetrics(t, reader)
	for _, name := range []string{
		"messaging.process.duration",
		"messaging.client.operation.duration",
		"xkafka.transaction.duration",
	} {
		point := onlyHistogramPoint(t, durationHistogram(t, metrics[name]))
		if !slices.Equal(point.Bounds, want) {
			t.Fatalf("metric %q bounds = %v, want %v", name, point.Bounds, want)
		}
	}
}
