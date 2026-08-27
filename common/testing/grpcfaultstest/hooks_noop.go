//go:build !test_dep

package grpcfaultstest

import (
	"go.temporal.io/server/common/rpc/grpcfaults"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// NewGenerator returns nil when test hooks are disabled.
func NewGenerator(commontesthooks.TestHooks) grpcfaults.Generator {
	return nil
}
