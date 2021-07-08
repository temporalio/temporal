//+build wireinject

package matching

import "github.com/google/wire"


func ServiceFactory() {
	logger := log.With(params.Logger, tag.Service(serviceName))
	return InitializeMatchingService(logger)
}

// todomigryz: implement this method. Replace NewService method.
// todomigryz: Need to come up with proper naming convention for initialize vs factory methods.
func InitializeMatchingService(logger *log.Logger) (*Service, error) {
	wire.Build(
			MatchingServiceProvider,
		)
	return nil, nil
}
