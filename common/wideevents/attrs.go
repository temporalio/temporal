package wideevents

import (
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// jsonAttr records v as a compact JSON string under key rather than a nested structure, so the
// logging / ingestion layer keeps one low-cardinality field (e.g. details) instead of flattening
// into a dynamic key per leaf path. Used for composite fields (maps/slices/whole objects).
func jsonAttr(key string, v any) attribute.KeyValue {
	if b, err := json.Marshal(v); err == nil {
		return attribute.String(key, string(b))
	}
	return attribute.String(key, fmt.Sprintf("%v", v))
}
