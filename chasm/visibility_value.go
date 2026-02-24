package chasm

import (
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type VisibilityValue interface {
	MustEncode() *commonpb.Payload
	Equal(VisibilityValue) bool
	Value() any
}

type VisibilityValueInt int

func (v VisibilityValueInt) MustEncode() *commonpb.Payload {
	p, _ := payload.Encode(int(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_INT)
	return p
}

func (v VisibilityValueInt) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueInt)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueInt) Value() any {
	return int(v)
}

type VisibilityValueInt32 int32

func (v VisibilityValueInt32) MustEncode() *commonpb.Payload {
	p, _ := payload.Encode(int32(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_INT)
	return p
}

func (v VisibilityValueInt32) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueInt32)
	if !ok {
		return false
	}
	return v == ov
}

func (v VisibilityValueInt32) Value() any {
	return int32(v)
}

type VisibilityValueInt64 int64

func (v VisibilityValueInt64) MustEncode() *commonpb.Payload {
	p, _ := payload.Encode(int64(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_INT)
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
	p := payload.EncodeString(string(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_KEYWORD)
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
	p, _ := payload.Encode(bool(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_BOOL)
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
	p, _ := payload.Encode(float64(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_DOUBLE)
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
	p, _ := payload.Encode(time.Time(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_DATETIME)
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

type VisibilityValueByteSlice []byte

func (v VisibilityValueByteSlice) MustEncode() *commonpb.Payload {
	return payload.EncodeBytes([]byte(v))
}

func (v VisibilityValueByteSlice) Equal(other VisibilityValue) bool {
	ov, ok := other.(VisibilityValueByteSlice)
	if !ok {
		return false
	}
	return slices.Equal(v, ov)
}

func (v VisibilityValueByteSlice) Value() any {
	return []byte(v)
}

type VisibilityValueStringSlice []string

func (v VisibilityValueStringSlice) MustEncode() *commonpb.Payload {
	p, _ := payload.Encode([]string(v))
	sadefs.SetMetadataType(p, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
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
