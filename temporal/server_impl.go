package temporal

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.uber.org/multierr"
)

type (
	// ServerImpl is temporal server.
	ServerImpl struct {
		so               *serverOptions
		servicesMetadata []*ServicesMetadata
		stoppedCh        chan any
		logger           log.Logger
		namespaceLogger  resource.NamespaceLogger

		bootstrapPersistenceFactory persistenceClient.Factory
	}
)

// When starting multiple services in one process (typically a development server), start them
// in this order and stop them in the reverse order. This most important part here is that the
// worker depends on the frontend, which depends on matching and history.
var initOrder = map[primitives.ServiceName]int{
	primitives.MatchingService:         1,
	primitives.HistoryService:          2,
	primitives.InternalFrontendService: 3,
	primitives.FrontendService:         3,
	primitives.WorkerService:           4,
}

// NewServerFxImpl returns a new instance of server that serves one or many services.
func NewServerFxImpl(
	opts *serverOptions,
	logger log.Logger,
	namespaceLogger resource.NamespaceLogger,
	stoppedCh chan any,
	servicesGroup ServicesGroupIn,
	bootstrapPersistenceFactory persistenceClient.Factory,
) *ServerImpl {
	s := &ServerImpl{
		so:                          opts,
		stoppedCh:                   stoppedCh,
		logger:                      logger,
		namespaceLogger:             namespaceLogger,
		bootstrapPersistenceFactory: bootstrapPersistenceFactory,
	}
	for _, svcMeta := range servicesGroup.Services {
		if svcMeta != nil {
			s.servicesMetadata = append(s.servicesMetadata, svcMeta)
		}
	}
	return s
}

func (s *ServerImpl) Start(ctx context.Context) error {
	s.logger.Info("Starting server for services", tag.Value(s.so.serviceNames))
	s.logger.Debug(s.so.config.String())

	if err := initSystemNamespaces(
		ctx,
		s.so.config.ClusterMetadata.CurrentClusterName,
		s.bootstrapPersistenceFactory,
	); err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	return s.startServices()
}

func (s *ServerImpl) Stop(ctx context.Context) error {
	close(s.stoppedCh)

	svcs := slices.Clone(s.servicesMetadata)
	slices.SortFunc(svcs, func(a, b *ServicesMetadata) int {
		return -cmp.Compare(initOrder[a.serviceName], initOrder[b.serviceName]) // note negative
	})
	for _, svc := range svcs {
		svc.Stop(ctx)
	}

	if s.so.metricHandler != nil {
		s.so.metricHandler.Stop(s.logger)
	}
	return nil
}

func (s *ServerImpl) startServices() error {
	// The membership join time may exceed the configured max join duration.
	// Double the service start timeout to make sure there is enough time for start logic.
	timeout := max(serviceStartTimeout, 2*s.so.config.Global.Membership.MaxJoinDuration)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	svcs := slices.Clone(s.servicesMetadata)
	slices.SortFunc(svcs, func(a, b *ServicesMetadata) int {
		return cmp.Compare(initOrder[a.serviceName], initOrder[b.serviceName])
	})

	var allErrs error
	for _, svc := range svcs {
		err := svc.app.Start(ctx)
		if err != nil {
			allErrs = multierr.Append(allErrs, fmt.Errorf("failed to start service %v: %w", svc.serviceName, err))
		}
	}
	return allErrs
}

func initSystemNamespaces(
	ctx context.Context,
	currentClusterName string,
	factory persistenceClient.Factory,
) error {
	metadataManager, err := factory.NewMetadataManager()
	if err != nil {
		return fmt.Errorf("unable to initialize metadata manager: %w", err)
	}
	// Do not close the manager because the factory owns and closes its persistence resources.
	ctx, cancel := context.WithTimeout(
		headers.SetCallerInfo(ctx, headers.SystemBackgroundHighCallerInfo),
		30*time.Second,
	)
	defer cancel()

	if err = metadataManager.InitializeSystemNamespaces(ctx, currentClusterName); err != nil {
		return fmt.Errorf("unable to register system namespace: %w", err)
	}
	return nil
}
