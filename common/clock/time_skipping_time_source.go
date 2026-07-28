package clock

import "time"

var _ TimeSource = (*TimeSkippingTimeSourceWrapper)(nil)

// TimeSkippingTimeSourceWrapper wraps a base TimeSource and adds an offset
// returned by getOffset to Now() and Since(). The offset is read lazily on
// every call so callers can back it with live state (e.g. a field on
// MutableState) without re-wrapping when that state changes.
//
// AfterFunc and NewTimer delegate to the base TimeSource and are not
// affected by the offset.
type TimeSkippingTimeSourceWrapper struct {
	base      TimeSource
	getOffset func() time.Duration
}

// WrapTimeSourceWithTimeSkipping returns a TimeSource that adds getOffset() to
// the base's Now()/Since() on every call. If getOffset is nil, the wrapper behaves
// as a pass-through to base.
func WrapTimeSourceWithTimeSkipping(base TimeSource, getOffset func() time.Duration) TimeSource {
	return &TimeSkippingTimeSourceWrapper{base: base, getOffset: getOffset}
}

func (ts *TimeSkippingTimeSourceWrapper) Now() time.Time {
	t := ts.base.Now()
	if ts.getOffset != nil {
		t = t.Add(ts.getOffset())
	}
	return t
}

func (ts *TimeSkippingTimeSourceWrapper) Since(t time.Time) time.Duration {
	return ts.Now().Sub(t)
}

// AfterFunc delegates to the base TimeSource and does not apply the offset.
// TODO@time-skipping: examine if there is any need to skip time for this method.
func (ts *TimeSkippingTimeSourceWrapper) AfterFunc(d time.Duration, f func()) Timer {
	return ts.base.AfterFunc(d, f)
}

// NewTimer delegates to the base TimeSource and does not apply the offset.
// TODO@time-skipping: examine if there is any need to skip time for this method.
func (ts *TimeSkippingTimeSourceWrapper) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	return ts.base.NewTimer(d)
}
