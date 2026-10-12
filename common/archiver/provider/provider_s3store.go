//go:build !temporal_minimal || temporal_with_s3store

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/s3store"
)

func init() {
	optionalHistoryArchivers[s3store.URIScheme] = func(p *archiverProvider) (archiver.HistoryArchiver, error) {
		if p.historyArchiverConfigs.S3store == nil {
			return nil, ErrArchiverConfigNotFound
		}
		return s3store.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.S3store)
	}
	optionalVisibilityArchivers[s3store.URIScheme] = func(p *archiverProvider) (archiver.VisibilityArchiver, error) {
		if p.visibilityArchiverConfigs.S3store == nil {
			return nil, ErrArchiverConfigNotFound
		}
		return s3store.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.S3store)
	}
}
