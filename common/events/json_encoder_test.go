package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONEncoderRecordsTypedFields(t *testing.T) {
	enc := newJSONEncoder()
	enc.String("s", "v")
	enc.Int64("i", 42)
	enc.Float64("f", 1.5)
	enc.Bool("b", true)
	enc.Any("obj", map[string]int{"x": 1})
	enc.Any("nested", map[string]any{"config": map[string]any{"id": "u-123"}})

	require.Equal(t, "v", enc.fields["s"])
	require.Equal(t, int64(42), enc.fields["i"])
	require.InDelta(t, 1.5, enc.fields["f"], 1e-9)
	require.Equal(t, true, enc.fields["b"])
	// Any is rendered as a compact JSON string (avoids top-level cardinality / leaf-path flattening).
	require.Equal(t, `{"x":1}`, enc.fields["obj"])
	nested, ok := enc.fields["nested"].(string)
	require.True(t, ok)
	require.JSONEq(t, `{"config":{"id":"u-123"}}`, nested)
}
