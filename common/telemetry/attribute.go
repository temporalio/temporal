package telemetry

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	logtag "go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// LogKey is a type wrapper for the key part of a common/log/tag.Tag that
	// can be used to create full common/log/tag.Tag instances. It is intended
	// to be used as a const global and shared throughout the codebase.
	LogKey string

	// MetricsKey is a type wrapper for the key part of a common/metrics.Tag that
	// can be used to create full common/metrics.Tag instances. It is intended
	// to be used as a const global and shared throughout the codebase.
	MetricsKey string

	// Key is a type that brings some consistency to the server's use of log,
	// trace, and metrics annotations. The three tagging systems currently in
	// use are not compatible so we provide this type to at least normalize
	// naming. Consumers are expected to create package-global Key instances and
	// then use those instances to create values as appropriate to the context.
	Key struct {
		OTEL    attribute.Key
		Log     LogKey
		Metrics MetricsKey
	}

	mtag struct {
		k, v string
	}
)

func (t mtag) Key() string   { return t.k }
func (t mtag) Value() string { return t.v }

// NewKey creates a new Key instance, inserting the "io.temporal." prefix for
// OTEL attributes if it is absent.
func NewKey(str string) Key {
	fqstr := str
	if !strings.HasPrefix(str, "io.temporal") {
		fqstr = fmt.Sprintf("io.temporal.%s", str)
	}
	return Key{
		OTEL:    attribute.Key(fqstr),
		Log:     LogKey(str),
		Metrics: MetricsKey(str),
	}
}

// String creates a new string-valued logging tag
func (k LogKey) String(v string) logtag.Tag {
	return logtag.NewStringTag(string(k), v)
}

// Int creates a new int-valued logging tag
func (k LogKey) Int(v int) logtag.Tag {
	return logtag.NewInt(string(k), v)
}

// String creates a new string-valued metrics tag
func (k MetricsKey) String(v string) metrics.Tag {
	return mtag{k: string(k), v: v}
}
