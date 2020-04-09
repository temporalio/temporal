package history

import (
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/persistence"
)

type transactionPolicy int

const (
	transactionPolicyActive  transactionPolicy = 0
	transactionPolicyPassive transactionPolicy = 1
)

func (policy transactionPolicy) ptr() *transactionPolicy {
	return &policy
}

// NOTE: do not use make(type, len(input))
// since this will assume initial length being len(inputs)
// always use make(type, 0, len(input))

func convertPendingActivityInfos(
	inputs map[int64]*persistence.ActivityInfo,
) []*persistence.ActivityInfo {

	outputs := make([]*persistence.ActivityInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateActivityInfos(
	inputs map[*persistence.ActivityInfo]struct{},
) []*persistence.ActivityInfo {

	outputs := make([]*persistence.ActivityInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertDeleteActivityInfos(
	inputs map[int64]struct{},
) []int64 {

	outputs := make([]int64, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertSyncActivityInfos(
	activityInfos map[int64]*persistence.ActivityInfo,
	inputs map[int64]struct{},
) []persistence.Task {
	outputs := make([]persistence.Task, 0, len(inputs))
	for item := range inputs {
		activityInfo, ok := activityInfos[item]
		if ok {
			outputs = append(outputs, &persistence.SyncActivityTask{
				Version:     activityInfo.Version,
				ScheduledID: activityInfo.ScheduleID,
			})
		}
	}
	return outputs
}

func convertPendingTimerInfos(
	inputs map[string]*persistenceblobs.TimerInfo,
) []*persistenceblobs.TimerInfo {

	outputs := make([]*persistenceblobs.TimerInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateTimerInfos(
	inputs map[*persistenceblobs.TimerInfo]struct{},
) []*persistenceblobs.TimerInfo {

	outputs := make([]*persistenceblobs.TimerInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertDeleteTimerInfos(
	inputs map[string]struct{},
) []string {

	outputs := make([]string, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingChildExecutionInfos(
	inputs map[int64]*persistence.ChildExecutionInfo,
) []*persistence.ChildExecutionInfo {

	outputs := make([]*persistence.ChildExecutionInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateChildExecutionInfos(
	inputs map[*persistence.ChildExecutionInfo]struct{},
) []*persistence.ChildExecutionInfo {

	outputs := make([]*persistence.ChildExecutionInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingRequestCancelInfos(
	inputs map[int64]*persistenceblobs.RequestCancelInfo,
) []*persistenceblobs.RequestCancelInfo {

	outputs := make([]*persistenceblobs.RequestCancelInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateRequestCancelInfos(
	inputs map[*persistenceblobs.RequestCancelInfo]struct{},
) []*persistenceblobs.RequestCancelInfo {

	outputs := make([]*persistenceblobs.RequestCancelInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingSignalInfos(
	inputs map[int64]*persistenceblobs.SignalInfo,
) []*persistenceblobs.SignalInfo {

	outputs := make([]*persistenceblobs.SignalInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateSignalInfos(
	inputs map[*persistenceblobs.SignalInfo]struct{},
) []*persistenceblobs.SignalInfo {

	outputs := make([]*persistenceblobs.SignalInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertSignalRequestedIDs(
	inputs map[string]struct{},
) []string {

	outputs := make([]string, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}
