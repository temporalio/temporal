package tag

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// ZapTag is the wrapper over zap.Field.
	ZapTag struct {
		// keep this field private
		field zap.Field
	}
)

// NewZapTag creates new ZapTag from zap.Field.
func NewZapTag(field zap.Field) ZapTag {
	return ZapTag{
		field: field,
	}
}

func (t ZapTag) Field() zap.Field {
	return t.field
}

func (t ZapTag) Key() string {
	return t.field.Key
}

func (t ZapTag) Value() interface{} {
	// Not for production use.
	enc := zapcore.NewMapObjectEncoder()
	t.field.AddTo(enc)
	for _, val := range enc.Fields {
		return val
	}
	return nil
}

func NewStringTag(key string, value string) ZapTag {
	return ZapTag{
		field: zap.String(key, value),
	}
}

func NewStringsTag(key string, value []string) ZapTag {
	return ZapTag{
		field: zap.Strings(key, value),
	}
}

// NewStringerTag returns a tag that will lazily generate the string representation
// of the provided fmt.Stringer value. Note that it does **not** cache the result, so
// you should use `NewStringTag` instead if the tag is applied to the logger itself using
// `log.With`.
//
// These are still useful if the String() implementation is complicated, especially if
// you have lots of Debug-level logs that are ignored in production.
func NewStringerTag(key string, value fmt.Stringer) ZapTag {
	return ZapTag{
		field: zap.Stringer(key, value),
	}
}

// NewStringersTag returns a tag that will lazily generate the string representation
// of the provided fmt.Stringer values. Note that it does **not** cache the results, so
// you should use `NewStringsTag` instead if the tag is applied to the logger itself using
// `log.With`.
//
// These are still useful if the String() implementation is complicated, especially if
// you have lots of Debug-level logs that are ignored in production.
func NewStringersTag(key string, value []fmt.Stringer) ZapTag {
	return ZapTag{
		field: zap.Stringers(key, value),
	}
}

func NewInt64(key string, value int64) ZapTag {
	return ZapTag{
		field: zap.Int64(key, value),
	}
}

func NewInt(key string, value int) ZapTag {
	return ZapTag{
		field: zap.Int(key, value),
	}
}

func NewInt32(key string, value int32) ZapTag {
	return ZapTag{
		field: zap.Int32(key, value),
	}
}

func NewUInt32(key string, value uint32) ZapTag {
	return ZapTag{
		field: zap.Uint32(key, value),
	}
}

func NewFloat64(key string, value float64) ZapTag {
	return ZapTag{
		field: zap.Float64(key, value),
	}
}

func NewBoolTag(key string, value bool) ZapTag {
	return ZapTag{
		field: zap.Bool(key, value),
	}
}

func NewErrorTag(key string, value error) ZapTag {
	return ZapTag{
		field: zap.NamedError(key, value),
	}
}

func NewDurationTag(key string, value time.Duration) ZapTag {
	return ZapTag{
		field: zap.Duration(key, value),
	}
}

func NewDurationPtrTag(key string, value *durationpb.Duration) ZapTag {
	return ZapTag{
		field: zap.Duration(key, value.AsDuration()),
	}
}

func NewTimeTag(key string, value time.Time) ZapTag {
	return ZapTag{
		field: zap.Time(key, value),
	}
}

func NewTimePtrTag(key string, value *timestamppb.Timestamp) ZapTag {
	return ZapTag{
		field: zap.Time(key, value.AsTime()),
	}
}

func NewAnyTag(key string, value interface{}) ZapTag {
	return ZapTag{
		field: zap.Any(key, value),
	}
}

func NewBinaryTag(key string, value []byte) ZapTag {
	return ZapTag{
		field: zap.Binary(key, value),
	}
}
