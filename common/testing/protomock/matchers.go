// Proto-type adapters for the mock library
package protomock

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/server/common/testing/protodiff"
	"go.uber.org/mock/gomock"
)

type protoEq struct {
	want any
	opts []cmp.Option
}

type protoSliceEq struct {
	want []any
	opts []cmp.Option
}

// Eq returns a gomock.Matcher that checks equality with proto semantics for any proto messages inside the value.
// Pass cmp options, e.g. cmp.AllowUnexported, for types with unexported fields.
func Eq(want any, opts ...cmp.Option) gomock.Matcher {
	return protoEq{want, opts}
}

// Matches returns true when the provided value equals the expectation
func (m protoEq) Matches(x any) bool {
	if m.want == nil && x == nil {
		return true
	}

	return protodiff.Equal(m.want, x, m.opts...)
}

func (m protoEq) String() string {
	return fmt.Sprintf("is equal to %v (%T)", m.want, m.want)
}

// Got is used by gomock to print the received value in a failure message.
func (m protoEq) Got(got any) string {
	return fmt.Sprintf("%v (%T)\nDiff (-want +got):\n%v", got, got, protodiff.Diff(m.want, got, m.opts...))
}

func SliceEq(want []any, opts ...cmp.Option) gomock.Matcher {
	return protoSliceEq{want, opts}
}

func (m protoSliceEq) Matches(x any) bool {
	xs, ok := x.([]any)
	if !ok || len(m.want) != len(xs) {
		return false
	}

	for i := range m.want {
		if !protodiff.Equal(m.want[i], xs[i], m.opts...) {
			return false
		}
	}
	return true
}

func (m protoSliceEq) String() string {
	return fmt.Sprintf("is equal to %v", m.want)
}
