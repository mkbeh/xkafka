package logger

import "log/slog"

// all key constants must be defined with the "Key" suffix.

// Constants and constructors to standardize the keys used in logs.

const (
	// errorKey - to pass error to log.
	errorKey string = "error"
	// componentKey - to identify the component that is logging (e.g.: "kafka-consumer")
	componentKey = "component"
)

// Error for passing error to log.
func Error(err error) slog.Attr { return slog.Any(errorKey, err) }

// Component to identify the component that is logging (e.g.: "kafka-consumer").
// here any distinct level of your application abstraction can be used.
// it's not mandatory associated with kind of an external connection,
// e.g.: "location-event-processor" is also applicable for the field.
func Component(component string) slog.Attr { return slog.String(componentKey, component) }

// Record transforms an []byte into a slog.Attr.
func Record(record []byte) slog.Attr { return slog.String("record", string(record)) }

// Records transforms a []byte into a slog.Attr.
func Records(records []byte) slog.Attr { return slog.String("records", string(records)) }

// Count transforms an int into a slog.Attr.
func Count(count int) slog.Attr { return slog.Int("count", count) }
