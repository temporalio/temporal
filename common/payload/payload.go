package payload

import (
	"bytes"
	"maps"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/util"
)

var (
	defaultDataConverter = converter.GetDefaultDataConverter()

	nilPayload, _        = Encode(nil)           // Data: nil
	nilSlicePayload, _   = Encode([]string(nil)) // Data: "null" (nil value is json encoded as null)
	emptySlicePayload, _ = Encode([]string{})    // Data: "[]"
)

func EncodeString(str string) *commonpb.Payload {
	// Error can be safely ignored here because string always can be converted to JSON
	p, _ := defaultDataConverter.ToPayload(str)
	return p
}

func EncodeBytes(bytes []byte) *commonpb.Payload {
	// Error can be safely ignored here because []byte always can be raw encoded
	p, _ := defaultDataConverter.ToPayload(bytes)
	return p
}

func Encode(value any) (*commonpb.Payload, error) {
	return defaultDataConverter.ToPayload(value)
}

func Decode(p *commonpb.Payload, valuePtr any) error {
	return defaultDataConverter.FromPayload(p, valuePtr)
}

func ToString(p *commonpb.Payload) string {
	return defaultDataConverter.ToString(p)
}

// MergeMapOfPayload returns a new map resulting from merging map `src` into `dst`.
// If a key in `src` already exists in `dst`, then the value in `src` replaces
// the value in `dst`.
// If a key in `src` has nil or empty slice payload value, then it deletes
// the key from `dst` if it exists.
// For example:
//
//	dst := map[string]*commonpb.Payload{
//		"key1": EncodeString("value1"),
//		"key2": EncodeString("value2"),
//	}
//	src := map[string]*commonpb.Payload{
//		"key1": EncodeString("newValue1"),
//		"key2": nilPayload,
//	}
//	res := MergeMapOfPayload(dst, src)
//
// The resulting map `res` is:
//
//	map[string]*commonpb.Payload{
//		"key1": EncodeString("newValue1"),
//	}
func MergeMapOfPayload(
	dst map[string]*commonpb.Payload,
	src map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {
	if src == nil {
		return maps.Clone(dst)
	}
	res := util.CloneMapNonNil(dst)
	for k, v := range src {
		if isNilPayload(v) {
			delete(res, k)
		} else {
			res[k] = v
		}
	}
	return res
}

// isNilPayload checks if the payload is equivalent to nil.
// There are four cases:
// - payload object is nil
// - payload's data is nil
// - payload's data is "null" (json encoded value for nil objects)
// - payload's data is "[]" (empty slice for backwards compatibility)
func isNilPayload(p *commonpb.Payload) bool {
	return p == nil ||
		bytes.Equal(p.Data, nilPayload.Data) ||
		bytes.Equal(p.Data, nilSlicePayload.Data) ||
		bytes.Equal(p.Data, emptySlicePayload.Data)
}

// FilterNilSearchAttributes returns a new SearchAttributes with nil/empty payload values filtered out.
// If the input is nil or all values are nil/empty, returns nil.
// This is used to filter out nil search attributes from workflow start and continue-as-new events.
// Reuses MergeMapOfPayload which already handles nil payload filtering.
func FilterNilSearchAttributes(sa *commonpb.SearchAttributes) *commonpb.SearchAttributes {
	if sa == nil || len(sa.GetIndexedFields()) == 0 {
		return nil
	}

	filtered := MergeMapOfPayload(nil, sa.GetIndexedFields())
	if len(filtered) == 0 {
		return nil
	}
	return &commonpb.SearchAttributes{IndexedFields: filtered}
}

// FilterNilMemo returns a new Memo with nil/empty payload values filtered out.
// If the input is nil or all values are nil/empty, returns nil.
// This is used to filter out nil memo fields from workflow start, continue-as-new, and modify-properties events.
// Reuses MergeMapOfPayload which already handles nil payload filtering.
func FilterNilMemo(memo *commonpb.Memo) *commonpb.Memo {
	if memo == nil || len(memo.GetFields()) == 0 {
		return nil
	}

	filtered := MergeMapOfPayload(nil, memo.GetFields())
	if len(filtered) == 0 {
		return nil
	}
	return &commonpb.Memo{Fields: filtered}
}
