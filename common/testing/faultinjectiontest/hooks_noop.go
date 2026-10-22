//go:build !test_dep

package faultinjectiontest

import (
	"go.temporal.io/server/common/faultinjection"
	"go.temporal.io/server/common/testing/testhooks"
)

func NewServiceGenerator(testhooks.TestHooks) faultinjection.Generator[any, any] { return nil }

func NewServiceCallbackGenerator(testhooks.TestHooks) *faultinjection.CallbackGenerator[any, any] {
	return faultinjection.NewCallbackGenerator[any, any]()
}
