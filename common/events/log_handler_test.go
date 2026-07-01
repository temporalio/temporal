package events

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
)

// captureLogger records the last Info call for assertions.
type captureLogger struct {
	msg  string
	tags []tag.Tag
}

func (c *captureLogger) Debug(string, ...tag.Tag) {}
func (c *captureLogger) Info(msg string, t ...tag.Tag) {
	c.msg = msg
	c.tags = t
}
func (c *captureLogger) Warn(string, ...tag.Tag)   {}
func (c *captureLogger) Error(string, ...tag.Tag)  {}
func (c *captureLogger) DPanic(string, ...tag.Tag) {}
func (c *captureLogger) Panic(string, ...tag.Tag)  {}
func (c *captureLogger) Fatal(string, ...tag.Tag)  {}

type sampleEvent struct {
	A string
	B int64
}

func (e sampleEvent) Encode(enc Encoder) {
	enc.String("a", e.A)
	enc.Int64("b", e.B)
}

func TestLogHandlerEmitsEventWithFields(t *testing.T) {
	lg := &captureLogger{}
	h := NewLogHandler(lg)

	h.Event("sample").Emit(sampleEvent{A: "x", B: 7})

	// The event type is now the log message itself, for easy tracing/grepping.
	require.Equal(t, "sample", lg.msg)
	var foundPayload bool
	for _, tg := range lg.tags {
		if tg.Key() == "payload" {
			payload, ok := tg.Value().(map[string]any)
			require.True(t, ok)
			require.Equal(t, "x", payload["a"])
			require.Equal(t, int64(7), payload["b"])
			foundPayload = true
		}
	}
	require.True(t, foundPayload, "payload tag present")
}

func TestLogHandlerWithTagsAddsCommonFields(t *testing.T) {
	lg := &captureLogger{}
	h := NewLogHandler(lg).WithTags(Tag{Key: "service_name", Value: "history"})

	h.Event("sample").Emit(sampleEvent{A: "x", B: 1})

	for _, tg := range lg.tags {
		if tg.Key() == "payload" {
			payload := tg.Value().(map[string]any)
			require.Equal(t, "history", payload["service_name"])
			return
		}
	}
	t.Fatal("payload tag not found")
}
