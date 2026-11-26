package fc

import (
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

const defaultCancelReservationTimeout = 5 * time.Second

var ReadinessCancelReservationTimeout = dynamicconfig.NewGlobalDurationSetting(
	"matching.fc.cancelReservationTimeout",
	defaultCancelReservationTimeout,
	`Timeout for best-effort cancellation of a concurrency reservation during rollback.`,
)

type readinessConfig struct {
	cancelReservationTimeout dynamicconfig.DurationPropertyFn
}

func NewReadinessConfig(dc *dynamicconfig.Collection) readinessConfig {
	return readinessConfig{
		cancelReservationTimeout: ReadinessCancelReservationTimeout.Get(dc),
	}
}
