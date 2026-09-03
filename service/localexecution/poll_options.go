package localexecution

import (
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

func ValidatePollOptions(
	options *workflowservice.LocalExecutionPollOptions,
	minimumSyncInterval time.Duration,
	maximumSyncInterval time.Duration,
	maximumIDLength int,
) error {
	if options == nil {
		return nil
	}
	if options.GetLocalServerId() == "" {
		return serviceerror.NewInvalidArgument("local execution server ID is required")
	}
	if len(options.GetLocalServerId()) > maximumIDLength {
		return serviceerror.NewInvalidArgument("local execution server ID exceeds the maximum length")
	}
	if options.GetProtocolVersion() != ProtocolVersion {
		return serviceerror.NewInvalidArgumentf(
			"unsupported local execution protocol version: %d",
			options.GetProtocolVersion(),
		)
	}
	if minimumSyncInterval <= 0 || maximumSyncInterval < minimumSyncInterval {
		return serviceerror.NewInternal("invalid local execution synchronization interval configuration")
	}
	if options.GetSyncInterval() == nil {
		return serviceerror.NewInvalidArgument("local execution synchronization interval is required")
	}
	if err := options.GetSyncInterval().CheckValid(); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid local execution synchronization interval: %v", err)
	}
	syncInterval := options.GetSyncInterval().AsDuration()
	if syncInterval < minimumSyncInterval || syncInterval > maximumSyncInterval {
		return serviceerror.NewInvalidArgumentf(
			"local execution synchronization interval must be between %s and %s",
			minimumSyncInterval,
			maximumSyncInterval,
		)
	}
	if options.GetRequestedLeaseDuration() == nil {
		return serviceerror.NewInvalidArgument("local execution lease duration is required")
	}
	if err := options.GetRequestedLeaseDuration().CheckValid(); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid local execution lease duration: %v", err)
	}
	if options.GetRequestedLeaseDuration().AsDuration() != 3*syncInterval {
		return serviceerror.NewInvalidArgument("local execution lease duration must be three times the synchronization interval")
	}
	return nil
}
