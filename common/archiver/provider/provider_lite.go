//go:build lite

package provider

import (
	"errors"
	"sync"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

var (
	ErrUnknownScheme          = errors.New("unknown archiver scheme")
	ErrArchiverConfigNotFound = errors.New("unable to find archiver config for the given scheme")
)

type (
	ArchiverProvider interface {
		GetHistoryArchiver(scheme string) (archiver.HistoryArchiver, error)
		GetVisibilityArchiver(scheme string) (archiver.VisibilityArchiver, error)
	}

	archiverProvider struct {
		sync.RWMutex

		historyArchiverConfigs    *config.HistoryArchiverProvider
		visibilityArchiverConfigs *config.VisibilityArchiverProvider

		executionManager persistence.ExecutionManager
		logger           log.Logger
		metricsHandler   metrics.Handler

		historyArchivers    map[string]archiver.HistoryArchiver
		visibilityArchivers map[string]archiver.VisibilityArchiver
	}
)

func NewArchiverProvider(
	historyArchiverConfigs *config.HistoryArchiverProvider,
	visibilityArchiverConfigs *config.VisibilityArchiverProvider,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
) ArchiverProvider {
	return &archiverProvider{
		historyArchiverConfigs:    historyArchiverConfigs,
		visibilityArchiverConfigs: visibilityArchiverConfigs,
		executionManager:          executionManager,
		logger:                    logger,
		metricsHandler:            metricsHandler,
		historyArchivers:          make(map[string]archiver.HistoryArchiver),
		visibilityArchivers:       make(map[string]archiver.VisibilityArchiver),
	}
}

func (p *archiverProvider) GetHistoryArchiver(scheme string) (archiver.HistoryArchiver, error) {
	p.RLock()
	if h, ok := p.historyArchivers[scheme]; ok {
		p.RUnlock()
		return h, nil
	}
	p.RUnlock()

	var (
		historyArchiver archiver.HistoryArchiver
		err             error
	)
	switch scheme {
	case filestore.URIScheme:
		if p.historyArchiverConfigs.Filestore == nil {
			return nil, ErrArchiverConfigNotFound
		}
		historyArchiver, err = filestore.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Filestore)
	default:
		return nil, ErrUnknownScheme
	}
	if err != nil {
		return nil, err
	}
	p.Lock()
	defer p.Unlock()
	if h, ok := p.historyArchivers[scheme]; ok {
		return h, nil
	}
	p.historyArchivers[scheme] = historyArchiver
	return historyArchiver, nil
}

func (p *archiverProvider) GetVisibilityArchiver(scheme string) (archiver.VisibilityArchiver, error) {
	p.RLock()
	if v, ok := p.visibilityArchivers[scheme]; ok {
		p.RUnlock()
		return v, nil
	}
	p.RUnlock()

	var (
		visibilityArchiver archiver.VisibilityArchiver
		err                error
	)
	switch scheme {
	case filestore.URIScheme:
		if p.visibilityArchiverConfigs.Filestore == nil {
			return nil, ErrArchiverConfigNotFound
		}
		visibilityArchiver, err = filestore.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Filestore)
	default:
		return nil, ErrUnknownScheme
	}
	if err != nil {
		return nil, err
	}
	p.Lock()
	defer p.Unlock()
	if v, ok := p.visibilityArchivers[scheme]; ok {
		return v, nil
	}
	p.visibilityArchivers[scheme] = visibilityArchiver
	return visibilityArchiver, nil
}
