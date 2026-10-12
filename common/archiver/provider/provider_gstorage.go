//go:build !temporal_minimal || temporal_with_gstorage

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud"
)

func init() {
	optionalHistoryArchivers[gcloud.URIScheme] = func(p *archiverProvider) (archiver.HistoryArchiver, error) {
		if p.historyArchiverConfigs.Gstorage == nil {
			return nil, ErrArchiverConfigNotFound
		}

		return gcloud.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Gstorage)
	}
	optionalVisibilityArchivers[gcloud.URIScheme] = func(p *archiverProvider) (archiver.VisibilityArchiver, error) {
		if p.visibilityArchiverConfigs.Gstorage == nil {
			return nil, ErrArchiverConfigNotFound
		}
		return gcloud.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Gstorage)
	}
}
