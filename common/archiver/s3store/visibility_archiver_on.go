//go:build !disable_cloud_archival

package s3store

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// NewVisibilityArchiver creates a new archiver.VisibilityArchiver based on s3
func NewVisibilityArchiver(
	logger log.Logger,
	metricsHandler metrics.Handler,
	cfg *config.S3Archiver,
) (archiver.VisibilityArchiver, error) {
	return newVisibilityArchiver(logger, metricsHandler, cfg)
}
