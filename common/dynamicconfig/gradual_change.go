package dynamicconfig

import (
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
)

// GradualChange represents a setting that can change its value over time in a controlled way.
// The value of a GradualChange is a function of a key and the current time. Before Start, the
// value is always Old for all keys, and after End it's always New. Between them, each key will
// change once at a specific time (returned by When).
//
// A setting with type GradualChange can use ConvertGradualChange as a conversion function to
// handle plain values of the underlying type as well as GradualChange structs.
type GradualChange[T any] struct {
	Old, New   T
	Start, End time.Time
}

// ConstantGradualChange returns a GradualChange whose Value always returns def and whose When
// always returns a time in the past.
func ConstantGradualChange[T any](def T) GradualChange[T] {
	return GradualChange[T]{New: def}
}

// Value returns the value for the given key at the given time.
func (c *GradualChange[T]) Value(key []byte, now time.Time) T {
	if !now.After(c.Start) {
		return c.Old
	} else if !now.Before(c.End) {
		return c.New
	}
	fraction := float64(now.Sub(c.Start)) / float64(c.End.Sub(c.Start))
	threshold := uint32(fraction * float64(math.MaxUint32))
	if farm.Fingerprint32(key) < threshold {
		return c.New
	}
	return c.Old
}

// When returns the time when the value for key will switch from old to new.
func (c *GradualChange[T]) When(key []byte) time.Time {
	fraction := float64(farm.Fingerprint32(key)) / float64(math.MaxUint32)
	when := time.Duration(fraction * float64(c.Start.Sub(c.End)))
	return c.Start.Add(when)
}

// ConvertGradualChange is a conversion function that can handle a plain T (which represents a
// constant value) as well as a GradualChange[T]. It can be used to turn settings that were not
// of type GradualChange into a GradualChange.
func ConvertGradualChange[T any](def T) func(v any) (GradualChange[T], error) {
	changeConverter := ConvertStructure(ConstantGradualChange(def))

	switch reflect.TypeFor[T]() {
	case reflect.TypeFor[bool]():
		return func(v any) (GradualChange[T], error) {
			if b, err := convertBool(v); err == nil {
				var change GradualChange[T]
				reflect.ValueOf(&change.New).Elem().SetBool(b)
				return change, nil
			}
			return changeConverter(v)
		}
	case reflect.TypeFor[int]():
		return func(v any) (GradualChange[T], error) {
			if i, err := convertInt(v); err == nil {
				var change GradualChange[T]
				reflect.ValueOf(&change.New).Elem().SetInt(int64(i))
				return change, nil
			}
			return changeConverter(v)
		}
	case reflect.TypeFor[float64]():
		return func(v any) (GradualChange[T], error) {
			if f, err := convertFloat(v); err == nil {
				var change GradualChange[T]
				reflect.ValueOf(&change.New).Elem().SetFloat(f)
				return change, nil
			}
			return changeConverter(v)
		}
	case reflect.TypeFor[string]():
		return func(v any) (GradualChange[T], error) {
			if s, err := convertString(v); err == nil {
				var change GradualChange[T]
				reflect.ValueOf(&change.New).Elem().SetString(s)
				return change, nil
			}
			return changeConverter(v)
		}
	case reflect.TypeFor[time.Duration]():
		return func(v any) (GradualChange[T], error) {
			if d, err := convertDuration(v); err == nil {
				var change GradualChange[T]
				reflect.ValueOf(&change.New).Elem().SetInt(int64(d))
				return change, nil
			}
			return changeConverter(v)
		}
	default:
		valueConverter := ConvertStructure(def)
		return func(v any) (GradualChange[T], error) {
			// both valueConverter and structConverter will probably return success here, so
			// we can't rely on the error. check if the input has "new" (the only required
			// key in a GradualChange).
			if vmap, ok := v.(map[string]any); ok {
				for k := range vmap {
					if strings.ToLower(k) == "new" {
						return changeConverter(v)
					}
				}
			}
			nv, err := valueConverter(v)
			return ConstantGradualChange(nv), err
		}
	}
}
