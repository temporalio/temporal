//go:build !disable_cloud_archival

package s3store

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

// NewHistoryArchiver creates a new archiver.HistoryArchiver based on s3
func NewHistoryArchiver(
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
	cfg *config.S3Archiver,
) (archiver.HistoryArchiver, error) {
	return newHistoryArchiver(executionManager, logger, metricsHandler, cfg, nil)
}
