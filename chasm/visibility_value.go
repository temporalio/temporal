package chasm

import (
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type VisibilityValue interface {
	MustEncode() *commonpb.Payload
	Equal(VisibilityValue) bool
	Value() any
}

type VisibilityValueInt64 int64

func (v VisibilityValueInt64) MustEncode() *commonpb.Payload {
	return sadefs.MustEncodeValue(int64(v), enumspb.INDEXED_VALUE_TYPE_INT)
}

func (v VisibilityValueInt64) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueInt64)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueInt64) Value() any {
	return int64(v)
}

type VisibilityValueKeyword string

func (v VisibilityValueKeyword) MustEncode() *commonpb.Payload {
	return sadefs.MustEncodeValue(string(v), enumspb.INDEXED_VALUE_TYPE_KEYWORD)
}

func (v VisibilityValueKeyword) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueKeyword)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueKeyword) Value() any {
	return string(v)
}

type VisibilityValueBool bool

func (v VisibilityValueBool) MustEncode() *commonpb.Payload {
	return sadefs.MustEncodeValue(bool(v), enumspb.INDEXED_VALUE_TYPE_BOOL)
}

func (v VisibilityValueBool) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueBool)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueBool) Value() any {
	return bool(v)
}

type VisibilityValueFloat64 float64

func (v VisibilityValueFloat64) MustEncode() *commonpb.Payload {
	return sadefs.MustEncodeValue(float64(v), enumspb.INDEXED_VALUE_TYPE_DOUBLE)
}

func (v VisibilityValueFloat64) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueFloat64)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueFloat64) Value() any {
	return float64(v)
}

type VisibilityValueTime time.Time

func (v VisibilityValueTime) MustEncode() *commonpb.Payload {
	return sadefs.MustEncodeValue(time.Time(v), enumspb.INDEXED_VALUE_TYPE_DATETIME)
}

func (v VisibilityValueTime) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueTime)
	if !ok {
		return false
	}
	return time.Time(v).Equal(time.Time(ov))
}

func (v VisibilityValueTime) Value() any {
	return time.Time(v)
}

type VisibilityValueStringSlice []string

func (v VisibilityValueStringSlice) MustEncode() *commonpb.Payload {
	return sadefs.MustEncodeValue([]string(v), enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
}

func (v VisibilityValueStringSlice) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueStringSlice)
	if !ok {
		return false
	}
	return slices.Equal(v, ov)
}

func (v VisibilityValueStringSlice) Value() any {
	return []string(v)
}

func isVisibilityValueEqual(v1, v2 VisibilityValue) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	return v1.Equal(v2)
}

// visibilityValueFromPayload decoded payload based on type set in its metadata.
func visibilityValueFromPayload(payload *commonpb.Payload) (VisibilityValue, error) {
	value, err := sadefs.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false)
	if err != nil {
		return nil, err
	}

	switch val := value.(type) {
	case int64:
		return VisibilityValueInt64(val), nil
	case float64:
		return VisibilityValueFloat64(val), nil
	case bool:
		return VisibilityValueBool(val), nil
	case time.Time:
		return VisibilityValueTime(val), nil
	case string:
		// Try to parse as datetime first
		if parsedTime, err := time.Parse(time.RFC3339, val); err == nil {
			return VisibilityValueTime(parsedTime), nil
		}
		return VisibilityValueKeyword(val), nil
	case []string:
		return VisibilityValueStringSlice(val), nil
	default:
		// this should never happen given that DecodeValue did not return an error
		return nil, fmt.Errorf("unexpected search attribute value type %T", value)
	}
}
