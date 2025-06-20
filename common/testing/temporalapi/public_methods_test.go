package temporalapi_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/temporalapi"
)

type publicIface interface {
	A()
	C()
}

type noPublic struct {
}

func (n noPublic) b() {}

type somePublic struct {
	noPublic
}

func (s somePublic) A() {}
func (s somePublic) C() {}

func TestWalkExportedMethods(t *testing.T) {
	require := require.New(t)
	var iface publicIface
	for _, tc := range []struct {
		Name   string
		Given  any
		Expect []string
	}{{
		Name:   "No public methods",
		Given:  noPublic{},
		Expect: nil,
	}, {
		Name:   "Some public methods",
		Given:  somePublic{},
		Expect: []string{"A", "C"},
	}, {
		Name:   "Interface with methods",
		Given:  &iface,
		Expect: []string{"A", "C"},
	}} {
		var methods []string
		temporalapi.WalkExportedMethods(tc.Given, func(m reflect.Method) {
			methods = append(methods, m.Name)
		})

		require.Equal(tc.Expect, methods, tc.Name)
	}
}
