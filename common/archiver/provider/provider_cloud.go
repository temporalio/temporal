//go:build !gomad && !gomad_wasm_slim

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud"
	"go.temporal.io/server/common/archiver/s3store"
)

const (
	gcloudURIScheme = gcloud.URIScheme
	s3URIScheme     = s3store.URIScheme
)

func newGcloudHistoryArchiver(p *archiverProvider) (archiver.HistoryArchiver, error) {
	if p.historyArchiverConfigs.Gstorage == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return gcloud.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.Gstorage)
}

func newGcloudVisibilityArchiver(p *archiverProvider) (archiver.VisibilityArchiver, error) {
	if p.visibilityArchiverConfigs.Gstorage == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return gcloud.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.Gstorage)
}

func newS3HistoryArchiver(p *archiverProvider) (archiver.HistoryArchiver, error) {
	if p.historyArchiverConfigs.S3store == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return s3store.NewHistoryArchiver(p.executionManager, p.logger, p.metricsHandler, p.historyArchiverConfigs.S3store)
}

func newS3VisibilityArchiver(p *archiverProvider) (archiver.VisibilityArchiver, error) {
	if p.visibilityArchiverConfigs.S3store == nil {
		return nil, ErrArchiverConfigNotFound
	}
	return s3store.NewVisibilityArchiver(p.logger, p.metricsHandler, p.visibilityArchiverConfigs.S3store)
}
