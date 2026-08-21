package history

import (
	"context"
	"sync"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.temporal.io/server/common/wideevents"
)

type parentChildEventCapture struct {
	embedded.Logger

	mu      sync.RWMutex
	records []otellog.Record
}

func (c *parentChildEventCapture) Emit(_ context.Context, record otellog.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, record.Clone())
}

func (*parentChildEventCapture) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

func (c *parentChildEventCapture) snapshot() []otellog.Record {
	c.mu.RLock()
	defer c.mu.RUnlock()
	records := make([]otellog.Record, len(c.records))
	for i, record := range c.records {
		records[i] = record.Clone()
	}
	return records
}

func wideEventAttributes(record otellog.Record) map[string]otellog.Value {
	attributes := make(map[string]otellog.Value, record.AttributesLen())
	record.WalkAttributes(func(attribute otellog.KeyValue) bool {
		attributes[attribute.Key] = attribute.Value
		return true
	})
	return attributes
}

func parentChildOutcomes(capture *parentChildEventCapture) []string {
	var outcomes []string
	for _, record := range parentChildRecords(capture) {
		if outcome, ok := wideEventAttributes(record)["outcome"]; ok {
			outcomes = append(outcomes, outcome.AsString())
		}
	}
	return outcomes
}

func parentChildRecords(capture *parentChildEventCapture) []otellog.Record {
	var records []otellog.Record
	for _, record := range capture.snapshot() {
		if record.EventName() == wideevents.ParentChildLifecycleEventName {
			records = append(records, record)
		}
	}
	return records
}
