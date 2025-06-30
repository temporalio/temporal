//go:build !disable_cloud_archival

package gcloud

import (
	"context"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud/connector"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

// NewHistoryArchiver creates a new gcloud storage HistoryArchiver
func NewHistoryArchiver(
	executionManager persistence.ExecutionManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
	cfg *config.GstorageArchiver,
) (archiver.HistoryArchiver, error) {
	storage, err := connector.NewClient(context.Background(), cfg)
	if err == nil {
		return newHistoryArchiver(executionManager, logger, metricsHandler, nil, storage), nil
	}
	return nil, err
}
