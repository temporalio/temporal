//go:build !temporal_minimal || temporal_with_gstorage

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud"
)

const gstorageURIScheme = gcloud.URIScheme

func (p *archiverProvider) newGstorageHistoryArchiver() (archiver.HistoryArchiver, error) {
	if p.historyArchiverConfigs.Gstorage == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return gcloud.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Gstorage)
}

func (p *archiverProvider) newGstorageVisibilityArchiver() (archiver.VisibilityArchiver, error) {
	if p.visibilityArchiverConfigs.Gstorage == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return gcloud.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Gstorage)
}
