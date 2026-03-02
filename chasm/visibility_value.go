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

func VisibilityValueFromPayload(p *commonpb.Payload) (VisibilityValue, error) {
	v, err := sadefs.DecodeValue(p, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false)
	if err != nil {
		return nil, err
	}
	switch vt := v.(type) {
	case bool:
		return VisibilityValueBool(vt), nil
	case time.Time:
		return VisibilityValueTime(vt), nil
	case float64:
		return VisibilityValueFloat64(vt), nil
	case int64:
		return VisibilityValueInt64(vt), nil
	case string:
		return VisibilityValueString(vt), nil
	case []string:
		return VisibilityValueStringSlice(vt), nil
	default:
		// this should never happen given that DecodeValue did not return an error
		return nil, fmt.Errorf("unexpected search attribute value type %T", v)
	}
}

type VisibilityValueInt64 int64

func (v VisibilityValueInt64) MustEncode() *commonpb.Payload {
	p, _ := sadefs.EncodeValue(int64(v), enumspb.INDEXED_VALUE_TYPE_INT)
	return p
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

type VisibilityValueString string

func (v VisibilityValueString) MustEncode() *commonpb.Payload {
	p, _ := sadefs.EncodeValue(string(v), enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	return p
}

func (v VisibilityValueString) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueString)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueString) Value() any {
	return string(v)
}

type VisibilityValueBool bool

func (v VisibilityValueBool) MustEncode() *commonpb.Payload {
	p, _ := sadefs.EncodeValue(bool(v), enumspb.INDEXED_VALUE_TYPE_BOOL)
	return p
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
	p, _ := sadefs.EncodeValue(float64(v), enumspb.INDEXED_VALUE_TYPE_DOUBLE)
	return p
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
	p, _ := sadefs.EncodeValue(time.Time(v), enumspb.INDEXED_VALUE_TYPE_DATETIME)
	return p
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
	p, _ := sadefs.EncodeValue([]string(v), enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	return p
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
