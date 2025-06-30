//go:build !disable_cloud_archival

package gcloud

import (
	"context"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud/connector"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on filestore
func NewVisibilityArchiver(
	logger log.Logger,
	metricsHandler metrics.Handler,
	cfg *config.GstorageArchiver,
) (archiver.VisibilityArchiver, error) {
	storage, err := connector.NewClient(context.Background(), cfg)
	return newVisibilityArchiver(logger, metricsHandler, storage), err
}
