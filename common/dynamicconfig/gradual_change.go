package dynamicconfig

import (
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/clock"
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

// StaticGradualChange returns a GradualChange whose Value always returns def and whose When
// always returns a time in the past.
func StaticGradualChange[T any](def T) GradualChange[T] {
	return GradualChange[T]{New: def}
}

// Value returns the value for the given key at the given time.
func (c *GradualChange[T]) Value(key []byte, now time.Time) T {
	if !now.Before(c.End) {
		return c.New
	} else if !now.After(c.Start) {
		return c.Old
	}
	fraction := float64(now.Sub(c.Start)) / float64(c.End.Sub(c.Start))
	threshold := uint32(fraction * float64(math.MaxUint32))
	if farm.Fingerprint32(key) < threshold {
		return c.New
	}
	return c.Old
}

// When returns the time when the value for key will switch from old to new. It may be the zero
// time for a static GradualChange.
func (c *GradualChange[T]) When(key []byte) time.Time {
	fraction := float64(farm.Fingerprint32(key)) / float64(math.MaxUint32)
	when := time.Duration(fraction * float64(c.End.Sub(c.Start)))
	return c.Start.Add(when)
}

// ConvertGradualChange is a conversion function that can handle a plain T (which represents a
// static value) as well as a GradualChange[T]. It can be used to turn settings that were not
// of type GradualChange into a GradualChange.
// nolint:revive // cognitive-complexity // this looks complicated but each case is fairly simple
func ConvertGradualChange[T any](def T) func(v any) (GradualChange[T], error) {
	changeConverter := ConvertStructure(StaticGradualChange(def))

	// Call this once so that if it's going to panic, it panics at static init time.
	_, _ = changeConverter(nil)

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
		// nolint:forbidigo // this will be triggered from a static initializer before it can be triggered from production code
		panic("ConvertGradualChange can only be used with scalar types for now")
	}
}

// SubscribeGradualChange is a helper that allows subscribing to a GradualChange[T] as if it
// was a T. It handles setting a timer for when the setting may change in the future.
func SubscribeGradualChange[T any](
	subscribable TypedSubscribable[GradualChange[T]],
	changeKey []byte,
	callback func(T),
	timeSource clock.TimeSource,
) (T, func()) {
	w := &gradualChangeSubscribeWrapper[T]{changeKey: changeKey, callback: callback, clock: timeSource}

	w.lock.Lock()
	w.change, w.cancelSub = subscribable(w.changeCallback)
	val, _ := w.reevalLocked()
	w.lock.Unlock()

	return val, w.cancel
}

type gradualChangeSubscribeWrapper[T any] struct {
	// constant:
	changeKey []byte
	callback  func(T)
	clock     clock.TimeSource
	// mutable, protected by lock:
	lock      sync.Mutex
	cancelSub func()
	change    GradualChange[T]
	val       T
	tmr       clock.Timer
}

func (w *gradualChangeSubscribeWrapper[T]) changeCallback(change GradualChange[T]) {
	w.lock.Lock()
	w.change = change
	val, changed := w.reevalLocked()
	w.lock.Unlock()

	if changed {
		w.callback(val)
	}
}

func (w *gradualChangeSubscribeWrapper[T]) timerCallback() {
	w.lock.Lock()
	val, changed := w.reevalLocked()
	w.lock.Unlock()

	if changed {
		w.callback(val)
	}
}

func (w *gradualChangeSubscribeWrapper[T]) cancel() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.cancelSub()
	w.setTimerLocked(nil)
}

func (w *gradualChangeSubscribeWrapper[T]) reevalLocked() (T, bool) {
	now := w.clock.Now()

	var newTmr clock.Timer
	if at := w.change.When(w.changeKey); at.After(now) {
		newTmr = w.clock.AfterFunc(at.Sub(now), w.timerCallback)
	}
	w.setTimerLocked(newTmr)

	newVal := w.change.Value(w.changeKey, now)
	changed := !reflect.DeepEqual(w.val, newVal)
	w.val = newVal
	return w.val, changed
}

func (w *gradualChangeSubscribeWrapper[T]) setTimerLocked(newTmr clock.Timer) {
	if w.tmr != nil {
		w.tmr.Stop()
	}
	w.tmr = newTmr
}
