// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/uber/cadence/common/persistence"
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
	inputs map[string]*persistence.TimerInfo,
) []*persistence.TimerInfo {

	outputs := make([]*persistence.TimerInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateTimerInfos(
	inputs map[*persistence.TimerInfo]struct{},
) []*persistence.TimerInfo {

	outputs := make([]*persistence.TimerInfo, 0, len(inputs))
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
	inputs map[int64]*persistence.RequestCancelInfo,
) []*persistence.RequestCancelInfo {

	outputs := make([]*persistence.RequestCancelInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateRequestCancelInfos(
	inputs map[*persistence.RequestCancelInfo]struct{},
) []*persistence.RequestCancelInfo {

	outputs := make([]*persistence.RequestCancelInfo, 0, len(inputs))
	for item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertPendingSignalInfos(
	inputs map[int64]*persistence.SignalInfo,
) []*persistence.SignalInfo {

	outputs := make([]*persistence.SignalInfo, 0, len(inputs))
	for _, item := range inputs {
		outputs = append(outputs, item)
	}
	return outputs
}

func convertUpdateSignalInfos(
	inputs map[*persistence.SignalInfo]struct{},
) []*persistence.SignalInfo {

	outputs := make([]*persistence.SignalInfo, 0, len(inputs))
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
