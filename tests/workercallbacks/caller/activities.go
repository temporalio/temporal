package caller

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// onNexusOpCompleteActivity is a make believe worker callback.
func onNexusOpCompleteActivity(ctx context.Context) error {
	log := activity.GetLogger(ctx)
	log.Info("onNexusOpCompleteActivity called")

	return nil
}
