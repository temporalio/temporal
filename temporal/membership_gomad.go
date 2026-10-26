//go:build gomad

package temporal

import (
	"errors"

	"go.uber.org/fx"
)

// Ringpop membership reaches syscall through tchannel and thrift; the gomad
// build serves only clusters that supply StaticServiceHosts. The
// gomad_wasm_slim WASM guest links ringpop: its network calls stay on the
// modeled stream.
func defaultMembershipModule() fx.Option {
	return fx.Error(errors.New("ringpop membership is not built under the gomad build tag; supply StaticServiceHosts"))
}
