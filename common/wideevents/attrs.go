package wideevents

import (
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/log"
)

// stringSliceAttr records a []string as a native OTEL slice attribute.
func stringSliceAttr(key string, values []string) log.KeyValue {
	vals := make([]log.Value, len(values))
	for i, v := range values {
		vals[i] = log.StringValue(v)
	}
	return log.Slice(key, vals...)
}

// stringMapAttr records a map[string]string as a native OTEL map attribute.
func stringMapAttr(key string, m map[string]string) log.KeyValue {
	kvs := make([]log.KeyValue, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, log.String(k, v))
	}
	return log.Map(key, kvs...)
}

// jsonAttr records v as a compact JSON string under key rather than a nested structure, so the
// logging / ingestion layer keeps one low-cardinality field (e.g. details) instead of flattening
// into a dynamic key per leaf path. Used for composite fields (maps/slices/whole objects).
func jsonAttr(key string, v any) log.KeyValue {
	if b, err := json.Marshal(v); err == nil {
		return log.String(key, string(b))
	}
	return log.String(key, fmt.Sprintf("%v", v))
}
