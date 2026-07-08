package events

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
)

// captureLogger records emitted records for assertions.
type captureLogger struct {
	embedded.Logger
	records []log.Record
}

func (c *captureLogger) Emit(_ context.Context, r log.Record) { c.records = append(c.records, r) }
func (c *captureLogger) Enabled(context.Context, log.EnabledParameters) bool {
	return true
}

// captureProvider hands out a single captureLogger and records the scope name and options it was
// created with.
type captureProvider struct {
	embedded.LoggerProvider
	logger *captureLogger
	name   string
	cfg    log.LoggerConfig
}

func (p *captureProvider) Logger(name string, opts ...log.LoggerOption) log.Logger {
	p.name = name
	p.cfg = log.NewLoggerConfig(opts...)
	return p.logger
}

type sampleEvent struct {
	A string
	B int64
}

func (e sampleEvent) EventName() string { return "sample" }
func (e sampleEvent) Attributes() []log.KeyValue {
	return []log.KeyValue{log.String("a", e.A), log.Int64("b", e.B)}
}

func TestEmitWritesEventWithAttributes(t *testing.T) {
	lg := &captureLogger{}

	Emit(lg, sampleEvent{A: "x", B: 7})

	require.Len(t, lg.records, 1)
	rec := lg.records[0]
	// The event type is the record's event name, for easy tracing/grepping.
	require.Equal(t, "sample", rec.EventName())

	got := map[string]log.Value{}
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		got[kv.Key] = kv.Value
		return true
	})
	require.Equal(t, "x", got["a"].AsString())
	require.Equal(t, int64(7), got["b"].AsInt64())
}

func TestEmitNilLoggerIsNoop(t *testing.T) {
	require.NotPanics(t, func() {
		Emit(nil, sampleEvent{A: "x", B: 1})
	})
}

func TestNewLoggerAttachesServiceNameAsScopeAttribute(t *testing.T) {
	p := &captureProvider{logger: &captureLogger{}}
	_ = NewLogger(p, "history")

	require.Equal(t, instrumentationName, p.name)

	var found bool
	set := p.cfg.InstrumentationAttributes()
	for iter := set.Iter(); iter.Next(); {
		kv := iter.Attribute()
		if kv.Key == attribute.Key("service.name") {
			require.Equal(t, "history", kv.Value.AsString())
			found = true
		}
	}
	require.True(t, found, "service.name scope attribute present")
}

func TestNoopLoggerDiscards(t *testing.T) {
	require.NotPanics(t, func() {
		Emit(NoopLogger(), sampleEvent{A: "x", B: 1})
	})
}
