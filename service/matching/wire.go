//+build wireinject

package matching

import (
	"github.com/google/wire"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/config"
)


// todomigryz: implement this method. Replace NewService method.
// todomigryz: Need to come up with proper naming convention for initialize vs factory methods.
func InitializeMatchingService(
	logger log.Logger,
	params *resource.BootstrapParams,
	dcClient dynamicconfig.Client,
	metricsReporter metrics.Reporter,
	svcCfg config.Service,
) (*Service, error) {
	wire.Build(
		ServiceIdxProvider,
		ServiceConfigProvider,
		TaggedLoggerProvider,
		ThrottledLoggerProvider,
		MetricsReporterProvider,
		MetricsClientProvider,
		NewService,
	)
	return nil, nil
}
