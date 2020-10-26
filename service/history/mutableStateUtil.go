// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
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
	inputs map[int64]*persistencespb.ActivityInfo,
) []*persistencespb.ActivityInfo {

	outputs := make([]*persistencespb.ActivityInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateActivityInfos(
	inputs map[*persistencespb.ActivityInfo]struct{},
) []*persistencespb.ActivityInfo {

	outputs := make([]*persistencespb.ActivityInfo, 0, len(inputs))
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
	activityInfos map[int64]*persistencespb.ActivityInfo,
	inputs map[int64]struct{},
) []persistence.Task {
	outputs := make([]persistence.Task, 0, len(inputs))
	for item := range inputs {
		activityInfo, ok := activityInfos[item]
		if ok {
			outputs = append(outputs, &persistence.SyncActivityTask{
				Version:     activityInfo.Version,
				ScheduledID: activityInfo.ScheduleId,
			})
		}
	}
	return outputs
}

func convertPendingTimerInfos(
	inputs map[string]*persistencespb.TimerInfo,
) []*persistencespb.TimerInfo {

	outputs := make([]*persistencespb.TimerInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateTimerInfos(
	inputs map[*persistencespb.TimerInfo]struct{},
) []*persistencespb.TimerInfo {

	outputs := make([]*persistencespb.TimerInfo, 0, len(inputs))
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
	inputs map[int64]*persistencespb.ChildExecutionInfo,
) []*persistencespb.ChildExecutionInfo {

	outputs := make([]*persistencespb.ChildExecutionInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateChildExecutionInfos(
	inputs map[*persistencespb.ChildExecutionInfo]struct{},
) []*persistencespb.ChildExecutionInfo {

	outputs := make([]*persistencespb.ChildExecutionInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingRequestCancelInfos(
	inputs map[int64]*persistencespb.RequestCancelInfo,
) []*persistencespb.RequestCancelInfo {

	outputs := make([]*persistencespb.RequestCancelInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateRequestCancelInfos(
	inputs map[*persistencespb.RequestCancelInfo]struct{},
) []*persistencespb.RequestCancelInfo {

	outputs := make([]*persistencespb.RequestCancelInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingSignalInfos(
	inputs map[int64]*persistencespb.SignalInfo,
) []*persistencespb.SignalInfo {

	outputs := make([]*persistencespb.SignalInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateSignalInfos(
	inputs map[*persistencespb.SignalInfo]struct{},
) []*persistencespb.SignalInfo {

	outputs := make([]*persistencespb.SignalInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertStringSetToSlice(
	inputs map[string]struct{},
) []string {
	outputs := make([]string, len(inputs))
	i := 0
	for item := range inputs {
		outputs[i] = item
		i++
	}
	return outputs
}
func convertStringSliceToSet(
	inputs []string,
) map[string]struct{} {
	outputs := make(map[string]struct{}, len(inputs))
	for _, item := range inputs {
		outputs[item] = struct{}{}
	}
	return outputs
}
