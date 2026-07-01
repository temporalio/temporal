package events

import "encoding/json"

// jsonEncoder is an Encoder that accumulates fields into a map for JSON logging.
type jsonEncoder struct {
	fields map[string]any
}

func newJSONEncoder() *jsonEncoder {
	return &jsonEncoder{fields: make(map[string]any)}
}

func (e *jsonEncoder) String(key, value string)     { e.fields[key] = value }
func (e *jsonEncoder) Int64(key string, v int64)     { e.fields[key] = v }
func (e *jsonEncoder) Float64(key string, v float64) { e.fields[key] = v }
func (e *jsonEncoder) Bool(key string, v bool)       { e.fields[key] = v }
// Any records v as a compact JSON string rather than a nested object, so the logging /
// ingestion layer keeps one low-cardinality field (e.g. payload_details) instead of flattening
// into a dynamic key per leaf path. Other handlers may choose to encode Any structurally.
func (e *jsonEncoder) Any(key string, v any) {
	if b, err := json.Marshal(v); err == nil {
		e.fields[key] = string(b)
		return
	}
	e.fields[key] = v
}
