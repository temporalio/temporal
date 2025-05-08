// Proto-type adapters for the mock library
package protomock

import (
	"fmt"

	"go.temporal.io/api/temporalproto"
	"go.uber.org/mock/gomock"
)

type protoEq struct {
	want any
}

type protoSliceEq struct {
	want []any
}

// Eq returns a gomock.Matcher that uses proto.Equal to check equality
func Eq(want any) gomock.Matcher {
	return protoEq{want}
}

// Matches returns true when the provided value equals the expectation
func (m protoEq) Matches(x any) bool {
	if m.want == nil && x == nil {
		return true
	}

	return temporalproto.DeepEqual(m.want, x)
}

func (m protoEq) String() string {
	return fmt.Sprintf("is equal to %v (%T)", m.want, m.want)
}

func SliceEq(want []any) gomock.Matcher {
	return protoSliceEq{want}
}

func (m protoSliceEq) Matches(x any) bool {
	xs, ok := x.([]any)
	if !ok || len(m.want) != len(xs) {
		return false
	}

	for i := range m.want {
		if !temporalproto.DeepEqual(m.want[i], xs[i]) {
			return false
		}
	}
	return true
}

func (m protoSliceEq) String() string {
	return fmt.Sprintf("is equal to %v", m.want)
}
