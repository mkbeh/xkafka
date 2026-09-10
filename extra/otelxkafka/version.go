package otelxkafka

import "runtime/debug"

// ScopeName is the OpenTelemetry instrumentation scope name.
const ScopeName = "github.com/mkbeh/xkafka/extra/otelxkafka"

// Version returns the version of the otelxkafka instrumentation module.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	if info.Main.Path == ScopeName {
		return info.Main.Version
	}

	for _, dep := range info.Deps {
		if dep.Path == ScopeName {
			return dep.Version
		}
	}

	return "unknown"
}
