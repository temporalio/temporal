//go:build !temporal_minimal || temporal_with_azblob

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/azure_store"
)

func (p *archiverProvider) newAzblobHistoryArchiver() (archiver.HistoryArchiver, error) {
	if p.historyArchiverConfigs.Azblob == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return azure_store.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Azblob)
}

func (p *archiverProvider) newAzblobVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	if p.visibilityArchiverConfigs.Azblob == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return azure_store.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Azblob)
}
