//go:build lite

package gcloud

import (
	"errors"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

const URIScheme = "gs"

// NewHistoryArchiver returns an error because the gcloud implementation is disabled in lite builds.
func NewHistoryArchiver(persistence.ExecutionManager, log.Logger, metrics.Handler, *config.GstorageArchiver) (archiver.HistoryArchiver, error) {
	return nil, errors.New("gcloud archiver disabled in lite build")
}

// NewVisibilityArchiver returns an error because the gcloud implementation is disabled in lite builds.
func NewVisibilityArchiver(log.Logger, metrics.Handler, *config.GstorageArchiver) (archiver.VisibilityArchiver, error) {
	return nil, errors.New("gcloud archiver disabled in lite build")
}
