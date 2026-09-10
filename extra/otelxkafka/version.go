package otelxkafka

import "runtime/debug"

// semVersion returns the semantic version supplied to tracer and meter creation.
func semVersion() string {
	v := "unknown"

	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == instrumentationName {
				v = dep.Version
				break
			}
		}
	}

	return "semver:" + v
}
