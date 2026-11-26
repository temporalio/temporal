//go:build !temporal_minimal || temporal_with_ringpop

package temporal

import (
	"go.temporal.io/server/common/membership/ringpop"
	"go.uber.org/fx"
)

func defaultMembershipModule() fx.Option {
	return ringpop.MembershipModule
}

func (so *serverOptions) validateMembershipConfig() error {
	return nil
}
