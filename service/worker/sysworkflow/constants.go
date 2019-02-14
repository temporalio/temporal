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

type contextKey int

const (
	sysWorkerContainerKey contextKey = iota
)

const (
	// SystemDomainName is domain name for all system workflows
	SystemDomainName = "cadence-system"

	workflowIDPrefix                    = "cadsys-wf"
	decisionTaskList                    = "cadsys-decision-tl"
	workflowStartToCloseTimeout         = time.Hour * 24 * 30
	decisionTaskStartToCloseTimeout     = time.Minute
	signalName                          = "cadsys-signal-sig"
	signalsUntilContinueAsNew           = 1000
	archiveSystemWorkflowFnName         = "ArchiveSystemWorkflow"
	archivalUploadActivityFnName        = "ArchivalUploadActivity"
	archivalDeleteHistoryActivityFnName = "ArchivalDeleteHistoryActivity"
	historyBlobKeyExt                   = "history"
	blobstoreOperationsDefaultTimeout   = 5 * time.Second
	heartbeatTimeout                    = 10 * time.Second
	numWorkers                          = 50

	// the following are all non-retryable error strings
	errArchivalUploadActivityGetDomainStr           = "failed to get domain from domain cache"
	errArchivalUploadActivityNextBlobStr            = "failed to get next blob from iterator"
	errArchivalUploadActivityConstructKeyStr        = "failed to construct blob key"
	errArchivalUploadActivityBlobExistsStr          = "failed to check if blob exists already"
	errArchivalUploadActivityMarshalBlobStr         = "failed to marshal history blob"
	errArchivalUploadActivityConvertHeaderToTagsStr = "failed to convert header to tags"
	errArchivalUploadActivityWrapBlobStr            = "failed to wrap blob"
	errArchivalUploadActivityUploadBlobStr          = "failed to upload blob"
	errDeleteHistoryActivityDeleteFromV2Str         = "failed to delete history from events v2"
	errDeleteHistoryActivityDeleteFromV1Str         = "failed to delete history from events v1"
)
