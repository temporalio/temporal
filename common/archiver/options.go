package archiver

import (
	"context"
	"errors"

	"go.temporal.io/sdk/activity"
)

type (
	// ArchiveOption is used to provide options for adding features to
	// the Archive method of History/Visibility Archiver
	ArchiveOption func(featureCatalog *ArchiveFeatureCatalog)

	// ArchiveFeatureCatalog is a collection features for the Archive method of
	// History/Visibility Archiver
	ArchiveFeatureCatalog struct {
		ProgressManager   ProgressManager
		NonRetryableError NonRetryableError
	}

	// NonRetryableError returns an error indicating archiver has encountered an non-retryable error
	NonRetryableError func() error

	// ProgressManager is used to record and load archive progress
	ProgressManager interface {
		RecordProgress(ctx context.Context, progress interface{}) error
		LoadProgress(ctx context.Context, valuePtr interface{}) error
		HasProgress(ctx context.Context) bool
	}
)

// GetFeatureCatalog applies all the ArchiveOptions to the catalog and returns the catalog.
// It should be called inside the Archive method.
func GetFeatureCatalog(opts ...ArchiveOption) *ArchiveFeatureCatalog {
	catalog := &ArchiveFeatureCatalog{}
	for _, opt := range opts {
		opt(catalog)
	}
	return catalog
}

// GetHeartbeatArchiveOption returns an ArchiveOption for enabling heartbeating.
// It should be used when the Archive method is invoked inside an activity.
func GetHeartbeatArchiveOption() ArchiveOption {
	return func(catalog *ArchiveFeatureCatalog) {
		catalog.ProgressManager = &heartbeatProgressManager{}
	}
}

type heartbeatProgressManager struct{}

func (h *heartbeatProgressManager) RecordProgress(ctx context.Context, progress interface{}) error {
	activity.RecordHeartbeat(ctx, progress)
	return nil
}

func (h *heartbeatProgressManager) LoadProgress(ctx context.Context, valuePtr interface{}) error {
	if !h.HasProgress(ctx) {
		return errors.New("no progress information in the context")
	}
	return activity.GetHeartbeatDetails(ctx, valuePtr)
}

func (h *heartbeatProgressManager) HasProgress(ctx context.Context) bool {
	return activity.HasHeartbeatDetails(ctx)
}

// GetNonRetryableErrorOption returns an ArchiveOption so that archiver knows what should
// be returned when an non-retryable error is encountered.
func GetNonRetryableErrorOption(nonRetryableErr error) ArchiveOption {
	return func(catalog *ArchiveFeatureCatalog) {
		catalog.NonRetryableError = func() error {
			return nonRetryableErr
		}
	}
}
