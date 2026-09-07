package otelxkafka

import "testing"

func TestKotelHooks(t *testing.T) {
	meter := NewMeter()

	tracer := NewTracer()
	kotel := NewKotel(
		WithMeter(meter),
		WithTracer(tracer),
	)

	first := kotel.Hooks()
	second := kotel.Hooks()

	if len(first) != 2 {
		t.Fatalf("first hooks length = %d, want 2", len(first))
	}
	if len(second) != 2 {
		t.Fatalf("second hooks length = %d, want 2", len(second))
	}

	firstTracer, ok := first[0].(*Tracer)
	if !ok {
		t.Fatalf("first hook type = %T, want *Tracer", first[0])
	}
	firstMeter, ok := first[1].(*Meter)
	if !ok {
		t.Fatalf("second hook type = %T, want *Meter", first[1])
	}

	secondTracer, ok := second[0].(*Tracer)
	if !ok {
		t.Fatalf("first hook type = %T, want *Tracer", second[0])
	}
	secondMeter, ok := second[1].(*Meter)
	if !ok {
		t.Fatalf("second hook type = %T, want *Meter", second[1])
	}

	if firstTracer == secondTracer {
		t.Fatal("tracer hook instance was reused")
	}
	if firstMeter == secondMeter {
		t.Fatal("meter hook instance was reused")
	}
}

func TestKotelHooksEmpty(t *testing.T) {
	if hooks := NewKotel().Hooks(); len(hooks) != 0 {
		t.Fatalf("hooks length = %d, want 0", len(hooks))
	}
}
