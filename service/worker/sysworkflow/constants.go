// Copyright (c) 2017 Uber Technologies, Inc.
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

package sysworkflow

import (
	"time"
)

const (
	// Domain is the cadence system workflows domain
	Domain = "cadence-system"
	// DecisionTaskList is the task list that all system workflows share
	DecisionTaskList = "cadsys-decision-tl"
	// SignalName is the name of the cadence signal that system tasks are sent on
	SignalName = "cadsys-signal-sig"
	// WorkflowIDPrefix is the prefix of all system workflow ids
	WorkflowIDPrefix = "cadsys-wf"
	// SignalsUntilContinueAsNew is the number of signals system workflow must receive before continuing as new
	SignalsUntilContinueAsNew = 1000
	// SystemWorkflowScope scope for all metrics emitted by system workflow
	SystemWorkflowScope = "system-workflow"
	// SystemWorkflowIDTag tag for system workflowID
	SystemWorkflowIDTag = "system-workflow-id"
	// HandledSignalCount counter of number of signals processed by system workflow
	HandledSignalCount = "handled-signal"
	// UnknownSignalTypeErr counter of number of unknown signals received by system workflow
	UnknownSignalTypeErr = "unknown-signal-err"
	// ArchivalFailureErr counter of number of archival activity failures
	ArchivalFailureErr = "archival-failure"
	// BackfillFailureErr counter of number of backfill activity failures
	BackfillFailureErr = "backfill-failure"
	// ChannelClosedUnexpectedlyError counter of number of unexpected channel closes in system workflow
	ChannelClosedUnexpectedlyError = "channel-closed-unexpectedly-err"
	// ArchivalActivityFnName name of archival activity function
	ArchivalActivityFnName = "ArchivalActivity"
	// BackfillActivityFnName name of backfill activity function
	BackfillActivityFnName = "BackfillActivity"
	// SystemWorkflowFnName name of system workflow function
	SystemWorkflowFnName = "SystemWorkflow"
	// WorkflowStartToCloseTimeout is the time for the workflow to finish
	WorkflowStartToCloseTimeout = time.Hour * 24 * 30
	// DecisionTaskStartToCloseTimeout is the time for decision to finish
	DecisionTaskStartToCloseTimeout = time.Minute
	// DomainIDTag tag which identifies the domainID of an archived history
	DomainIDTag = "domainID"
	// WorkflowIDTag tag which identifies the workflowID of an archived history
	WorkflowIDTag = "workflowID"
	//RunIDTag tag which identifies the runID of an archived history
	RunIDTag = "runID"
	// BucketNameTag tag which identifies the bucket name
	BucketNameTag = "bucket-name"
)

// RequestType is the type for signals that can be sent to system workflows
type RequestType int

const (
	archivalRequest RequestType = iota
	backfillRequest
)

type contextKey int

const (
	blobstoreClientKey contextKey = iota
	frontendClientKey
)
