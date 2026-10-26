//go:build !gomad

package temporal

import (
	"go.temporal.io/server/common/membership/ringpop"
	"go.uber.org/fx"
)

func defaultMembershipModule() fx.Option {
	return ringpop.MembershipModule
}
