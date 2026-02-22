//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination provider_mock.go

package provider

import (
	"errors"
	"sync"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/gcloud"
	"go.temporal.io/server/common/archiver/s3store"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

var (
	// ErrUnknownScheme is the error for unknown archiver scheme
	ErrUnknownScheme = errors.New("unknown archiver scheme")
	// ErrArchiverConfigNotFound is the error for unable to find the config for an archiver given scheme
	ErrArchiverConfigNotFound = errors.New("unable to find archiver config for the given scheme")
)

type (
	// ArchiverProvider returns history or visibility archiver based on the scheme.
	// The archiver for each scheme will be created only once and cached.
	ArchiverProvider interface {
		GetHistoryArchiver(scheme string) (archiver.HistoryArchiver, error)
		GetVisibilityArchiver(scheme string) (archiver.VisibilityArchiver, error)
	}

	// NewCustomHistoryArchiverParams provides dependencies for constructing a history archiver.
	NewCustomHistoryArchiverParams struct {
		Scheme           string
		ExecutionManager persistence.ExecutionManager
		Logger           log.Logger
		MetricsHandler   metrics.Handler
		Configs          map[string]any
	}

	// NewCustomVisibilityArchiverParams provides dependencies for constructing a visibility archiver.
	NewCustomVisibilityArchiverParams struct {
		Scheme         string
		Logger         log.Logger
		MetricsHandler metrics.Handler
		Configs        map[string]any
	}

	// CustomHistoryArchiverFactory constructs a history archiver for the given scheme.
	// Return ErrUnknownScheme to fall back to the default implementation.
	// If a non-nil archiver is returned, it takes precedence over built-in schemes.
	CustomHistoryArchiverFactory interface {
		NewCustomHistoryArchiver(NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error)
	}

	// CustomVisibilityArchiverFactory constructs a visibility archiver for the given scheme.
	// Return ErrUnknownScheme to fall back to the default implementation.
	// If a non-nil archiver is returned, it takes precedence over built-in schemes.
	CustomVisibilityArchiverFactory interface {
		NewCustomVisibilityArchiver(NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error)
	}

	CustomHistoryArchiverFactoryFunc func(NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error)

	CustomVisibilityArchiverFactoryFunc func(NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error)

	archiverProvider struct {
		sync.RWMutex

		historyArchiverConfigs    *config.HistoryArchiverProvider
		visibilityArchiverConfigs *config.VisibilityArchiverProvider

		customHistoryArchiverFactory    CustomHistoryArchiverFactory
		customVisibilityArchiverFactory CustomVisibilityArchiverFactory

		executionManager persistence.ExecutionManager
		logger           log.Logger
		metricsHandler   metrics.Handler

		// Key for the archiver is scheme
		historyArchivers    map[string]archiver.HistoryArchiver
		visibilityArchivers map[string]archiver.VisibilityArchiver
	}
)

func (f CustomHistoryArchiverFactoryFunc) NewCustomHistoryArchiver(
	params NewCustomHistoryArchiverParams,
) (archiver.HistoryArchiver, error) {
	return f(params)
}

func (f CustomVisibilityArchiverFactoryFunc) NewCustomVisibilityArchiver(
	params NewCustomVisibilityArchiverParams,
) (archiver.VisibilityArchiver, error) {
	return f(params)
}

// NewArchiverProvider returns a new Archiver provider
func NewArchiverProvider(
	historyArchiverConfigs *config.HistoryArchiverProvider,
	visibilityArchiverConfigs *config.VisibilityArchiverProvider,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
) ArchiverProvider {
	return NewArchiverProviderWithFactories(
		historyArchiverConfigs,
		visibilityArchiverConfigs,
		nil,
		nil,
		executionManager,
		logger,
		metricsHandler,
	)
}

func NewArchiverProviderWithFactories(
	historyArchiverConfigs *config.HistoryArchiverProvider,
	visibilityArchiverConfigs *config.VisibilityArchiverProvider,
	customHistoryArchiverFactory CustomHistoryArchiverFactory,
	customVisibilityArchiverFactory CustomVisibilityArchiverFactory,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
) ArchiverProvider {
	return &archiverProvider{
		historyArchiverConfigs:          historyArchiverConfigs,
		visibilityArchiverConfigs:       visibilityArchiverConfigs,
		executionManager:                executionManager,
		logger:                          logger,
		metricsHandler:                  metricsHandler,
		customHistoryArchiverFactory:    customHistoryArchiverFactory,
		customVisibilityArchiverFactory: customVisibilityArchiverFactory,
		historyArchivers:                make(map[string]archiver.HistoryArchiver),
		visibilityArchivers:             make(map[string]archiver.VisibilityArchiver),
	}
}

func (p *archiverProvider) GetHistoryArchiver(scheme string) (historyArchiver archiver.HistoryArchiver, err error) {
	p.RLock()
	if historyArchiver, ok := p.historyArchivers[scheme]; ok {
		p.RUnlock()
		return historyArchiver, nil
	}
	p.RUnlock()

	if p.customHistoryArchiverFactory != nil {
		var customConfigs map[string]any
		if p.historyArchiverConfigs != nil {
			customConfigs = p.historyArchiverConfigs.CustomStores[scheme]
		}
		historyArchiver, err = p.customHistoryArchiverFactory.NewCustomHistoryArchiver(NewCustomHistoryArchiverParams{
			Scheme:           scheme,
			ExecutionManager: p.executionManager,
			Logger:           p.logger,
			MetricsHandler:   p.metricsHandler,
			Configs:          customConfigs,
		})
		if err != nil && !errors.Is(err, ErrUnknownScheme) {
			return nil, err
		}
	}

	if historyArchiver == nil {
		switch scheme {
		case filestore.URIScheme:
			if p.historyArchiverConfigs.Filestore == nil {
				return nil, ErrArchiverConfigNotFound
			}
			historyArchiver, err = filestore.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Filestore)

		case gcloud.URIScheme:
			if p.historyArchiverConfigs.Gstorage == nil {
				return nil, ErrArchiverConfigNotFound
			}

			historyArchiver, err = gcloud.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Gstorage)

		case s3store.URIScheme:
			if p.historyArchiverConfigs.S3store == nil {
				return nil, ErrArchiverConfigNotFound
			}
			historyArchiver, err = s3store.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.S3store)
		default:
			return nil, ErrUnknownScheme
		}
	}

	if err != nil {
		return nil, err
	}

	p.Lock()
	defer p.Unlock()
	if existingHistoryArchiver, ok := p.historyArchivers[scheme]; ok {
		return existingHistoryArchiver, nil
	}
	p.historyArchivers[scheme] = historyArchiver
	return historyArchiver, nil
}

func (p *archiverProvider) GetVisibilityArchiver(scheme string) (archiver.VisibilityArchiver, error) {
	p.RLock()
	if visibilityArchiver, ok := p.visibilityArchivers[scheme]; ok {
		p.RUnlock()
		return visibilityArchiver, nil
	}
	p.RUnlock()

	var visibilityArchiver archiver.VisibilityArchiver
	var err error

	if p.customVisibilityArchiverFactory != nil {
		var customConfigs map[string]any
		if p.visibilityArchiverConfigs != nil {
			customConfigs = p.visibilityArchiverConfigs.CustomStores[scheme]
		}
		visibilityArchiver, err = p.customVisibilityArchiverFactory.NewCustomVisibilityArchiver(NewCustomVisibilityArchiverParams{
			Scheme:         scheme,
			Logger:         p.logger,
			MetricsHandler: p.metricsHandler,
			Configs:        customConfigs,
		})
		if err != nil && !errors.Is(err, ErrUnknownScheme) {
			return nil, err
		}
	}

	if visibilityArchiver == nil {
		switch scheme {
		case filestore.URIScheme:
			if p.visibilityArchiverConfigs.Filestore == nil {
				return nil, ErrArchiverConfigNotFound
			}
			visibilityArchiver, err = filestore.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Filestore)
		case s3store.URIScheme:
			if p.visibilityArchiverConfigs.S3store == nil {
				return nil, ErrArchiverConfigNotFound
			}
			visibilityArchiver, err = s3store.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.S3store)
		case gcloud.URIScheme:
			if p.visibilityArchiverConfigs.Gstorage == nil {
				return nil, ErrArchiverConfigNotFound
			}
			visibilityArchiver, err = gcloud.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Gstorage)

		default:
			return nil, ErrUnknownScheme
		}
	}
	if err != nil {
		return nil, err
	}

	p.Lock()
	defer p.Unlock()
	if existingVisibilityArchiver, ok := p.visibilityArchivers[scheme]; ok {
		return existingVisibilityArchiver, nil
	}
	p.visibilityArchivers[scheme] = visibilityArchiver
	return visibilityArchiver, nil

}
