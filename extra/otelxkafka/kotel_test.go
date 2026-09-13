package otelxkafka

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

func TestKotelHooks(t *testing.T) {
	t.Parallel()

	meter := &Meter{}
	tracer := &Tracer{}
	hooks := NewKotel(
		WithMeter(meter),
		WithTracer(tracer),
		WithMeter(nil),
		WithTracer(nil),
	).Hooks()

	if len(hooks) != 2 {
		t.Fatalf("hooks = %d, want 2", len(hooks))
	}
	if hooks[0] != tracer {
		t.Fatal("first hook is not tracer")
	}
	if hooks[1] != meter {
		t.Fatal("second hook is not meter")
	}

	hooks = NewKotel(WithMeter(nil), WithTracer(nil)).Hooks()
	if len(hooks) != 0 {
		t.Fatalf("nil components produced %d hooks", len(hooks))
	}
}

func TestTelemetryConcurrentReuse(t *testing.T) {
	t.Parallel()

	meter, reader := newTestMeter(
		t,
		ClientID("shared"),
		ConsumerGroup("workers"),
		Labels(map[string]string{"env": "test"}),
	)
	tracer, recorder := newTestTracer(
		t,
		ClientID("shared"),
		ConsumerGroup("workers"),
		Labels(map[string]string{"env": "test"}),
	)

	const (
		workers    = 4
		iterations = 10
	)

	ctx := t.Context()
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := range workers {
		wg.Go(func() {
			<-start

			records := []*kgo.Record{{Topic: fmt.Sprintf("topic-%d", worker)}}
			for range iterations {
				handleCtx := tracer.OnHandleStart(ctx, records)
				meter.OnHandleEnd(handleCtx, records, time.Millisecond, nil)
				tracer.OnHandleEnd(handleCtx, records, time.Millisecond, nil)
			}
		})
	}

	close(start)
	wg.Wait()

	if got, want := len(recorder.Ended()), workers*iterations; got != want {
		t.Fatalf("ended spans = %d, want %d", got, want)
	}

	process := durationHistogram(t, collectMetrics(t, reader)["messaging.process.duration"])
	if len(process.DataPoints) != workers {
		t.Fatalf("process series = %d, want %d", len(process.DataPoints), workers)
	}

	wantTopics := make(map[string]struct{}, workers)
	for worker := range workers {
		wantTopics[fmt.Sprintf("topic-%d", worker)] = struct{}{}
	}

	for _, point := range process.DataPoints {
		topic, ok := point.Attributes.Value(semconv.MessagingDestinationNameKey)
		if !ok {
			t.Fatal("process series has no destination")
		}
		topicName := topic.AsString()
		if _, ok := wantTopics[topicName]; !ok {
			t.Fatalf("unexpected process destination %q", topicName)
		}
		delete(wantTopics, topicName)

		assertAttributes(
			t,
			point.Attributes.ToSlice(),
			semconv.MessagingClientID("shared"),
			semconv.MessagingConsumerGroupName("workers"),
			attribute.String("env", "test"),
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationName("process"),
			semconv.MessagingDestinationName(topicName),
		)
		assertHistogramPoint(t, point, iterations, iterations*0.001)
	}
}
