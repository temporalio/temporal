package events

import (
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/log"
)

// jsonAttr records v as a compact JSON string under key rather than a nested structure, so the
// logging / ingestion layer keeps one low-cardinality field (e.g. details) instead of flattening
// into a dynamic key per leaf path. Used for composite fields (maps/slices/whole objects).
func jsonAttr(key string, v any) log.KeyValue {
	if b, err := json.Marshal(v); err == nil {
		return log.String(key, string(b))
	}
	return log.String(key, fmt.Sprintf("%v", v))
}
