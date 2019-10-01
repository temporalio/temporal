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

package cli

import (
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	s "go.uber.org/cadence/.gen/go/shared"
)

const (
	localHostPort = "127.0.0.1:7933"

	maxOutputStringLength = 200 // max length for output string
	maxWorkflowTypeLength = 32  // max item length for output workflow type in table
	defaultMaxFieldLength = 500 // default max length for each attribute field
	maxWordLength         = 120 // if text length is larger than maxWordLength, it will be inserted spaces

	defaultTimeFormat                            = "15:04:05"   // used for converting UnixNano to string like 16:16:36 (only time)
	defaultDateTimeFormat                        = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
	defaultDomainRetentionDays                   = 3
	defaultContextTimeoutInSeconds               = 5
	defaultContextTimeout                        = defaultContextTimeoutInSeconds * time.Second
	defaultContextTimeoutForLongPoll             = 2 * time.Minute
	defaultContextTimeoutForListArchivedWorkflow = 5 * time.Minute

	defaultDecisionTimeoutInSeconds = 10
	defaultPageSizeForList          = 500
	defaultPageSizeForScan          = 2000
	defaultWorkflowIDReusePolicy    = s.WorkflowIdReusePolicyAllowDuplicateFailedOnly

	workflowStatusNotSet = -1
	showErrorStackEnv    = `CADENCE_CLI_SHOW_STACKS`

	searchAttrInputSeparator = "|"
)

var envKeysForUserName = []string{
	"USER",
	"LOGNAME",
	"HOME",
}

var resetTypesMap = map[string]string{
	"FirstDecisionCompleted": "",
	"LastDecisionCompleted":  "",
	"LastContinuedAsNew":     "",
	"BadBinary":              FlagResetBadBinaryChecksum,
}

type jsonType int

const (
	jsonTypeInput jsonType = iota
	jsonTypeMemo
)

var (
	cFactory ClientFactory

	colorRed     = color.New(color.FgRed).SprintFunc()
	colorMagenta = color.New(color.FgMagenta).SprintFunc()
	colorGreen   = color.New(color.FgGreen).SprintFunc()

	tableHeaderBlue         = tablewriter.Colors{tablewriter.FgHiBlueColor}
	optionErr               = "there is something wrong with your command options"
	osExit                  = os.Exit
	workflowClosedStatusMap = map[string]s.WorkflowExecutionCloseStatus{
		"completed":      s.WorkflowExecutionCloseStatusCompleted,
		"failed":         s.WorkflowExecutionCloseStatusFailed,
		"canceled":       s.WorkflowExecutionCloseStatusCanceled,
		"terminated":     s.WorkflowExecutionCloseStatusTerminated,
		"continuedasnew": s.WorkflowExecutionCloseStatusContinuedAsNew,
		"continueasnew":  s.WorkflowExecutionCloseStatusContinuedAsNew,
		"timedout":       s.WorkflowExecutionCloseStatusTimedOut,
		// below are some alias
		"c":         s.WorkflowExecutionCloseStatusCompleted,
		"complete":  s.WorkflowExecutionCloseStatusCompleted,
		"f":         s.WorkflowExecutionCloseStatusFailed,
		"fail":      s.WorkflowExecutionCloseStatusFailed,
		"cancel":    s.WorkflowExecutionCloseStatusCanceled,
		"terminate": s.WorkflowExecutionCloseStatusTerminated,
		"term":      s.WorkflowExecutionCloseStatusTerminated,
		"continue":  s.WorkflowExecutionCloseStatusContinuedAsNew,
		"cont":      s.WorkflowExecutionCloseStatusContinuedAsNew,
		"timeout":   s.WorkflowExecutionCloseStatusTimedOut,
	}
)
