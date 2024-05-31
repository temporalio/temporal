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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stream_receiver_flow_controller_mock.go
package replication

import (
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/configs"
)

type (
	FlowControlSignalProvider func() *FlowControlSignal

	// FlowControlSignal holds signals to make flow control decision, more signalsProvider can be added here i.e. total persistence rps, cpu usage etc.
	FlowControlSignal struct {
		taskTrackingCount int
	}
	ReceiverFlowController interface {
		GetFlowControlInfo(priority enums.TaskPriority) enums.ReplicationFlowControlCommand
	}
	streamReceiverFlowControllerImpl struct {
		signalsProvider map[enums.TaskPriority]FlowControlSignalProvider
		config          *configs.Config
	}
)

func NewReceiverFlowControl(signals map[enums.TaskPriority]FlowControlSignalProvider, config *configs.Config) *streamReceiverFlowControllerImpl {
	return &streamReceiverFlowControllerImpl{
		signalsProvider: signals,
		config:          config,
	}
}

func (s *streamReceiverFlowControllerImpl) GetFlowControlInfo(priority enums.TaskPriority) enums.ReplicationFlowControlCommand {
	if signal, ok := s.signalsProvider[priority]; ok {
		if signal().taskTrackingCount > s.config.ReplicationReceiverMaxOutstandingTaskCount() {
			return enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
		}
	}
	return enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
}
