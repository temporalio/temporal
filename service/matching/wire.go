//+build wireinject

package matching

import (
	"github.com/google/wire"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/resource"
)


// todomigryz: implement this method. Replace NewService method.
// todomigryz: Need to come up with proper naming convention for initialize vs factory methods.
func InitializeMatchingService(
	logger log.Logger,
	params *resource.BootstrapParams,
	dcClient dynamicconfig.Client) (*Service, error) {
	wire.Build(
			ServiceConfigProvider,
			NewService,
		)
	return nil, nil
}
