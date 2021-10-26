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

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/urfave/cli"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"

	clispb "go.temporal.io/server/api/cli/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/tools/cli/dataconverter"
	"go.temporal.io/server/tools/cli/stringify"
)

// ShowHistory shows the history of given workflow execution based on workflowID and runID.
func ShowHistory(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	showHistoryHelper(c, wid, rid)
}

// ShowHistoryWithWID shows the history of given workflow with workflow_id
func ShowHistoryWithWID(c *cli.Context) {
	if !c.Args().Present() {
		ErrorAndExit("Argument workflow_id is required.", nil)
	}
	wid := c.Args().First()
	rid := ""
	if c.NArg() >= 2 {
		rid = c.Args().Get(1)
	}
	showHistoryHelper(c, wid, rid)
}

func showHistoryHelper(c *cli.Context, wid, rid string) {
	sdkClient := getSDKClient(c)

	printDateTime := c.Bool(FlagPrintDateTime)
	printRawTime := c.Bool(FlagPrintRawTime)
	printFully := c.Bool(FlagPrintFullyDetail)
	printVersion := c.Bool(FlagPrintEventVersion)
	outputFileName := c.String(FlagOutputFilename)
	var maxFieldLength int
	if c.IsSet(FlagMaxFieldLength) || !printFully {
		maxFieldLength = c.Int(FlagMaxFieldLength)
	}
	resetPointsOnly := c.Bool(FlagResetPointsOnly)

	ctx, cancel := newContext(c)
	defer cancel()
	history, err := GetHistory(ctx, sdkClient, wid, rid)
	if err != nil {
		ErrorAndExit(fmt.Sprintf("Failed to get history on workflow id: %s, run id: %s.", wid, rid), err)
	}

	prevEvent := historypb.HistoryEvent{}
	if printFully { // dump everything
		for _, e := range history.Events {
			if resetPointsOnly {
				if prevEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
					prevEvent = *e
					continue
				}
				prevEvent = *e
			}
			fmt.Println(stringify.AnyToString(e, true, maxFieldLength, dataconverter.GetCurrent()))
		}
	} else if c.IsSet(FlagEventID) { // only dump that event
		eventID := c.Int(FlagEventID)
		if eventID <= 0 || eventID > len(history.Events) {
			ErrorAndExit("EventId out of range.", fmt.Errorf("number should be 1 - %d inclusive", len(history.Events)))
		}
		e := history.Events[eventID-1]
		fmt.Println(stringify.AnyToString(e, true, 0, dataconverter.GetCurrent()))
	} else { // use table to pretty output, will trim long text
		table := tablewriter.NewWriter(os.Stdout)
		table.SetBorder(false)
		table.SetColumnSeparator("")
		for _, e := range history.Events {
			if resetPointsOnly {
				if prevEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
					prevEvent = *e
					continue
				}
				prevEvent = *e
			}

			var columns []string
			columns = append(columns, convert.Int64ToString(e.GetEventId()))

			if printRawTime {
				columns = append(columns, convert.Int64ToString(timestamp.TimeValue(e.GetEventTime()).UnixNano()))
			} else if printDateTime {
				columns = append(columns, formatTime(timestamp.TimeValue(e.GetEventTime()), false))
			}
			if printVersion {
				columns = append(columns, fmt.Sprintf("(Version: %v)", e.Version))
			}

			columns = append(columns, ColorEvent(e), HistoryEventToString(e, false, maxFieldLength))
			table.Append(columns)
		}
		table.Render()
	}

	if outputFileName != "" {
		serializer := codec.NewJSONPBIndentEncoder(" ")
		data, err := serializer.Encode(history)
		if err != nil {
			ErrorAndExit("Failed to serialize history data.", err)
		}
		if err := os.WriteFile(outputFileName, data, 0666); err != nil {
			ErrorAndExit("Failed to export history data file.", err)
		}
	}
}

// StartWorkflow starts a new workflow execution
func StartWorkflow(c *cli.Context) {
	startWorkflowHelper(c, false)
}

// RunWorkflow starts a new workflow execution and print workflow progress and result
func RunWorkflow(c *cli.Context) {
	startWorkflowHelper(c, true)
}

func startWorkflowHelper(c *cli.Context, shouldPrintProgress bool) {
	sdkClient := getSDKClient(c)

	namespace := getRequiredGlobalOption(c, FlagNamespace)
	taskQueue := getRequiredOption(c, FlagTaskQueue)
	workflowType := getRequiredOption(c, FlagWorkflowType)
	et := c.Int(FlagWorkflowExecutionTimeout)
	rt := c.Int(FlagWorkflowRunTimeout)
	dt := c.Int(FlagWorkflowTaskTimeout)
	wid := c.String(FlagWorkflowID)
	if len(wid) == 0 {
		wid = uuid.New()
	}
	reusePolicy := defaultWorkflowIDReusePolicy
	if c.IsSet(FlagWorkflowIDReusePolicy) {
		reusePolicyInt, err := stringToEnum(c.String(FlagWorkflowIDReusePolicy), enumspb.WorkflowIdReusePolicy_value)
		if err != nil {
			ErrorAndExit("Failed to parse Reuse Policy", err)
		}
		reusePolicy = enumspb.WorkflowIdReusePolicy(reusePolicyInt)
	}

	inputs := unmarshalInputsFromCLI(c)
	wo := client.StartWorkflowOptions{
		ID:                       wid,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: time.Duration(et) * time.Second,
		WorkflowTaskTimeout:      time.Duration(dt) * time.Second,
		WorkflowRunTimeout:       time.Duration(rt) * time.Second,
		WorkflowIDReusePolicy:    reusePolicy,
	}
	if c.IsSet(FlagCronSchedule) {
		wo.CronSchedule = c.String(FlagCronSchedule)
	}

	wo.Memo = unmarshalMemoFromCLI(c)
	wo.SearchAttributes = unmarshalSearchAttrFromCLI(c)

	startFn := func() {
		tcCtx, cancel := newContext(c)
		defer cancel()
		resp, err := sdkClient.ExecuteWorkflow(tcCtx, wo, workflowType, inputs...)

		if err != nil {
			ErrorAndExit("Failed to create workflow.", err)
		} else {
			fmt.Printf("Started Workflow Id: %s, run Id: %s\n", wid, resp.GetRunID())
		}
	}

	runFn := func() {
		tcCtx, cancel := newContextForLongPoll(c)
		defer cancel()
		resp, err := sdkClient.ExecuteWorkflow(tcCtx, wo, workflowType, inputs...)

		if err != nil {
			ErrorAndExit("Failed to run workflow.", err)
		}

		// print execution summary
		fmt.Println(colorMagenta("Running execution:"))
		table := tablewriter.NewWriter(os.Stdout)
		executionData := [][]string{
			{"Workflow Id", wid},
			{"Run Id", resp.GetRunID()},
			{"Type", workflowType},
			{"Namespace", namespace},
			{"Task Queue", taskQueue},
			{"Args", truncate(formatInputsForDisplay(inputs))}, // in case of large input
		}
		table.SetBorder(false)
		table.SetColumnSeparator(":")
		table.AppendBulk(executionData) // Add Bulk Data
		table.Render()

		printWorkflowProgress(c, wid, resp.GetRunID())
	}

	if shouldPrintProgress {
		runFn()
	} else {
		startFn()
	}
}

func unmarshalInputsFromCLI(c *cli.Context) []interface{} {
	jsonsRaw := readJSONInputs(c)

	var result []interface{}
	for _, jsonRaw := range jsonsRaw {
		if jsonRaw == nil {
			result = append(result, nil)
		} else {
			var j interface{}
			if err := json.Unmarshal(jsonRaw, &j); err != nil {
				ErrorAndExit("Input is not valid JSON.", err)
			}
			result = append(result, j)
		}

	}

	return result
}

func formatInputsForDisplay(inputs []interface{}) string {
	var result []string
	for _, input := range inputs {
		s, _ := json.Marshal(input)
		result = append(result, string(s))
	}
	return fmt.Sprintf("[%s]", strings.Join(result, ","))
}

func unmarshalSearchAttrFromCLI(c *cli.Context) map[string]interface{} {
	sanitize := func(val string) []string {
		trimmedVal := strings.TrimSpace(val)
		if len(trimmedVal) == 0 {
			return nil
		}
		splitVal := strings.Split(trimmedVal, searchAttrInputSeparator)
		result := make([]string, len(splitVal))
		for i, v := range splitVal {
			result[i] = strings.TrimSpace(v)
		}
		return result
	}

	searchAttrKeys := sanitize(c.String(FlagSearchAttributeKey))
	if len(searchAttrKeys) == 0 {
		return nil
	}
	rawSearchAttrVals := sanitize(c.String(FlagSearchAttributeValue))
	if len(rawSearchAttrVals) == 0 {
		return nil
	}

	if len(searchAttrKeys) != len(rawSearchAttrVals) {
		ErrorAndExit(fmt.Sprintf("Uneven number of search attributes keys (%d): %v and values(%d): %v.", len(searchAttrKeys), searchAttrKeys, len(rawSearchAttrVals), rawSearchAttrVals), nil)
	}

	fields := make(map[string]interface{}, len(searchAttrKeys))

	for i, v := range rawSearchAttrVals {
		var j interface{}
		if err := json.Unmarshal([]byte(v), &j); err != nil {
			ErrorAndExit("Search attribute JSON parse error.", err)
		}
		fields[searchAttrKeys[i]] = j
	}

	return fields
}

func unmarshalMemoFromCLI(c *cli.Context) map[string]interface{} {
	// Memo flags were not passed => Memo is not provided.
	if !c.IsSet(FlagMemoKey) && !c.IsSet(FlagMemo) && !c.IsSet(FlagMemoFile) {
		return nil
	}

	if !c.IsSet(FlagMemoKey) {
		ErrorAndExit(fmt.Sprintf("Memo keys must be provided using %s.", FlagMemoKey), nil)
	}

	if c.IsSet(FlagMemo) && c.IsSet(FlagMemoFile) {
		ErrorAndExit(fmt.Sprintf("Only one of %s or %s should be used.", FlagMemo, FlagMemoFile), nil)
	}

	if !c.IsSet(FlagMemo) && !c.IsSet(FlagMemoFile) {
		ErrorAndExit(fmt.Sprintf("Memo values must be provided using %s or %s.", FlagMemo, FlagMemoFile), nil)
	}

	memoKeys := c.StringSlice(FlagMemoKey)

	var memoValues []string
	if c.IsSet(FlagMemoFile) {
		inputFile := c.String(FlagMemoFile)
		// This method is purely used to parse input from the CLI. The input comes from a trusted user
		// #nosec
		data, err := os.ReadFile(inputFile)
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Error reading memo file %s.", inputFile), err)
		}
		memoValues = strings.Split(string(data), "\n")
	} else if c.IsSet(FlagMemo) {
		memoValues = c.StringSlice(FlagMemo)
	}

	if len(memoKeys) != len(memoValues) {
		ErrorAndExit(fmt.Sprintf("Number of memo keys %d and values %d are not equal.", len(memoKeys), len(memoValues)), nil)
	}

	fields := make(map[string]interface{}, len(memoKeys))
	for i, key := range memoKeys {
		fields[key] = memoValues[i]
	}
	return fields
}

func getPrintableMemo(memo *commonpb.Memo) string {
	buf := new(bytes.Buffer)
	for k, v := range memo.Fields {
		var memo string
		err := payload.Decode(v, &memo)
		if err != nil {
			memo = "Memo is not a string"
		}
		_, _ = fmt.Fprintf(buf, "%s=%s\n", k, memo)
	}
	return buf.String()
}

func getPrintableSearchAttributes(searchAttributes *commonpb.SearchAttributes) string {
	var buf bytes.Buffer
	searchAttributesString, err := searchattribute.Stringify(searchAttributes, nil)
	if err != nil {
		fmt.Printf("%s: unable to stringify search attribute: %v\n",
			colorMagenta("Warning"),
			err)
	}

	for saName, saValueString := range searchAttributesString {
		_, _ = fmt.Fprintf(&buf, "%s=%s\n", saName, saValueString)
	}
	return buf.String()
}

// helper function to print workflow progress with time refresh every second
func printWorkflowProgress(c *cli.Context, wid, rid string) {
	fmt.Println(colorMagenta("Progress:"))

	sdkClient := getSDKClient(c)
	timeElapse := 1
	isTimeElapseExist := false
	doneChan := make(chan bool)
	var lastEvent *historypb.HistoryEvent // used for print result of this run
	ticker := time.NewTicker(time.Second).C

	tcCtx, cancel := newIndefiniteContext(c)
	defer cancel()

	showDetails := c.Bool(FlagShowDetail)
	var maxFieldLength int
	if c.IsSet(FlagMaxFieldLength) {
		maxFieldLength = c.Int(FlagMaxFieldLength)
	}

	go func() {
		iter := sdkClient.GetWorkflowHistory(tcCtx, wid, rid, true, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				ErrorAndExit("Unable to read event.", err)
			}
			if isTimeElapseExist {
				removePrevious2LinesFromTerminal()
				isTimeElapseExist = false
			}
			if showDetails {
				fmt.Printf("  %d, %s, %s, %s\n", event.GetEventId(), formatTime(timestamp.TimeValue(event.GetEventTime()), false), ColorEvent(event), HistoryEventToString(event, true, maxFieldLength))
			} else {
				fmt.Printf("  %d, %s, %s\n", event.GetEventId(), formatTime(timestamp.TimeValue(event.GetEventTime()), false), ColorEvent(event))
			}
			lastEvent = event
		}
		doneChan <- true
	}()

	for {
		select {
		case <-ticker:
			if isTimeElapseExist {
				removePrevious2LinesFromTerminal()
			}
			fmt.Printf("\nTime elapse: %ds\n", timeElapse)
			isTimeElapseExist = true
			timeElapse++
		case <-doneChan: // print result of this run
			fmt.Println(colorMagenta("\nResult:"))
			fmt.Printf("  Run Time: %d seconds\n", timeElapse)
			printRunStatus(lastEvent)
			return
		}
	}
}

// TerminateWorkflow terminates a workflow execution
func TerminateWorkflow(c *cli.Context) {
	sdkClient := getSDKClient(c)

	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	reason := c.String(FlagReason)

	ctx, cancel := newContext(c)
	defer cancel()
	err := sdkClient.TerminateWorkflow(ctx, wid, rid, reason, nil)

	if err != nil {
		ErrorAndExit("Terminate workflow failed.", err)
	} else {
		fmt.Println("Terminate workflow succeeded.")
	}
}

// CancelWorkflow cancels a workflow execution
func CancelWorkflow(c *cli.Context) {
	sdkClient := getSDKClient(c)

	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()
	err := sdkClient.CancelWorkflow(ctx, wid, rid)

	if err != nil {
		ErrorAndExit("Cancel workflow failed.", err)
	} else {
		fmt.Println("Cancel workflow succeeded.")
	}
}

// SignalWorkflow signals a workflow execution
func SignalWorkflow(c *cli.Context) {
	serviceClient := cFactory.FrontendClient(c)

	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	name := getRequiredOption(c, FlagName)
	input := processJSONInput(c)

	tcCtx, cancel := newContext(c)
	defer cancel()
	_, err := serviceClient.SignalWorkflowExecution(tcCtx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		SignalName: name,
		Input:      input,
		Identity:   getCliIdentity(),
	})

	if err != nil {
		ErrorAndExit("Signal workflow failed.", err)
	} else {
		fmt.Println("Signal workflow succeeded.")
	}
}

// QueryWorkflow query workflow execution
func QueryWorkflow(c *cli.Context) {
	getRequiredGlobalOption(c, FlagNamespace) // for pre-check and alert if not provided
	getRequiredOption(c, FlagWorkflowID)
	queryType := getRequiredOption(c, FlagQueryType)

	queryWorkflowHelper(c, queryType)
}

// QueryWorkflowUsingStackTrace query workflow execution using __stack_trace as query type
func QueryWorkflowUsingStackTrace(c *cli.Context) {
	queryWorkflowHelper(c, "__stack_trace")
}

func queryWorkflowHelper(c *cli.Context, queryType string) {
	serviceClient := cFactory.FrontendClient(c)

	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	input := processJSONInput(c)

	tcCtx, cancel := newContext(c)
	defer cancel()
	queryRequest := &workflowservice.QueryWorkflowRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		Query: &querypb.WorkflowQuery{
			QueryType: queryType,
		},
	}
	if input != nil {
		queryRequest.Query.QueryArgs = input
	}
	if c.IsSet(FlagQueryRejectCondition) {
		var rejectCondition enumspb.QueryRejectCondition
		switch c.String(FlagQueryRejectCondition) {
		case "not_open":
			rejectCondition = enumspb.QUERY_REJECT_CONDITION_NOT_OPEN
		case "not_completed_cleanly":
			rejectCondition = enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY
		default:
			ErrorAndExit(fmt.Sprintf("invalid reject condition %v, valid values are \"not_open\" and \"not_completed_cleanly\"", c.String(FlagQueryRejectCondition)), nil)
		}
		queryRequest.QueryRejectCondition = rejectCondition
	}
	queryResponse, err := serviceClient.QueryWorkflow(tcCtx, queryRequest)
	if err != nil {
		ErrorAndExit("Query workflow failed.", err)
		return
	}

	if queryResponse.QueryRejected != nil {
		fmt.Printf("Query was rejected, workflow has status: %v\n", queryResponse.QueryRejected.GetStatus())
	} else {
		queryResult := payloads.ToString(queryResponse.QueryResult)
		fmt.Printf("Query result:\n%v\n", queryResult)
	}
}

// ListWorkflow list workflow executions based on filters
func ListWorkflow(c *cli.Context) {
	more := c.Bool(FlagMore)
	queryOpen := c.Bool(FlagOpen)

	printJSON := c.Bool(FlagPrintJSON)
	printDecodedRaw := c.Bool(FlagPrintFullyDetail)

	if printJSON || printDecodedRaw {
		if !more {
			results, _ := getListResultInRaw(c, queryOpen, nil)
			fmt.Println("[")
			printListResults(results, printJSON, false)
			fmt.Println("]")
		} else {
			ErrorAndExit("Not support printJSON in more mode", nil)
		}
		return
	}

	table := createTableForListWorkflow(c, false, queryOpen)
	prepareTable := listWorkflow(c, table, queryOpen)

	if !more { // default mode only show one page items
		prepareTable(nil)
		table.Render()
	} else { // require input Enter to view next page
		var nextPageToken []byte
		for {
			nextPageToken, _ = prepareTable(nextPageToken)
			table.Render()
			table.ClearRows()

			if len(nextPageToken) == 0 {
				break
			}

			if !showNextPage() {
				break
			}
		}
	}
}

// ListAllWorkflow list all workflow executions based on filters
func ListAllWorkflow(c *cli.Context) {
	queryOpen := c.Bool(FlagOpen)

	printJSON := c.Bool(FlagPrintJSON)
	printDecodedRaw := c.Bool(FlagPrintFullyDetail)

	if printJSON || printDecodedRaw {
		var results []*workflowpb.WorkflowExecutionInfo
		var nextPageToken []byte
		fmt.Println("[")
		for {
			results, nextPageToken = getListResultInRaw(c, queryOpen, nextPageToken)
			printListResults(results, printJSON, nextPageToken != nil)
			if len(nextPageToken) == 0 {
				break
			}
		}
		fmt.Println("]")
		return
	}

	table := createTableForListWorkflow(c, true, queryOpen)
	prepareTable := listWorkflow(c, table, queryOpen)
	var nextPageToken []byte
	for {
		nextPageToken, _ = prepareTable(nextPageToken)
		if len(nextPageToken) == 0 {
			break
		}
	}
	table.Render()
}

// ScanAllWorkflow list all workflow executions using Scan API.
// It should be faster than ListAllWorkflow, but result are not sorted.
func ScanAllWorkflow(c *cli.Context) {
	printJSON := c.Bool(FlagPrintJSON)
	printDecodedRaw := c.Bool(FlagPrintFullyDetail)

	if printJSON || printDecodedRaw {
		var results []*workflowpb.WorkflowExecutionInfo
		var nextPageToken []byte
		fmt.Println("[")
		for {
			results, nextPageToken = getScanResultInRaw(c, nextPageToken)
			printListResults(results, printJSON, nextPageToken != nil)
			if len(nextPageToken) == 0 {
				break
			}
		}
		fmt.Println("]")
		return
	}

	table := createTableForListWorkflow(c, true, true)
	prepareTable := scanWorkflow(c, table, true)
	var nextPageToken []byte
	for {
		nextPageToken, _ = prepareTable(nextPageToken)
		if len(nextPageToken) == 0 {
			break
		}
	}
	table.Render()
}

// CountWorkflow count number of workflows
func CountWorkflow(c *cli.Context) {
	sdkClient := getSDKClient(c)

	query := c.String(FlagListQuery)
	request := &workflowservice.CountWorkflowExecutionsRequest{
		Query: query,
	}

	var count int64
	op := func() error {
		ctx, cancel := newContext(c)
		defer cancel()
		response, err := sdkClient.CountWorkflow(ctx, request)
		if err != nil {
			return err
		}
		count = response.GetCount()
		return nil
	}
	err := backoff.Retry(op, common.CreateFrontendServiceRetryPolicy(), common.IsContextDeadlineExceededErr)
	if err != nil {
		ErrorAndExit("Failed to count workflow.", err)
	}
	fmt.Println(count)
}

// ListArchivedWorkflow lists archived workflow executions based on filters
func ListArchivedWorkflow(c *cli.Context) {
	sdkClient := getSDKClient(c)

	printJSON := c.Bool(FlagPrintJSON)
	printDecodedRaw := c.Bool(FlagPrintFullyDetail)
	pageSize := c.Int(FlagPageSize)
	listQuery := getRequiredOption(c, FlagListQuery)
	printAll := c.Bool(FlagAll)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForList
	}

	request := &workflowservice.ListArchivedWorkflowExecutionsRequest{
		PageSize: int32(pageSize),
		Query:    listQuery,
	}

	contextTimeout := defaultContextTimeoutForListArchivedWorkflow
	if c.GlobalIsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}

	var result *workflowservice.ListArchivedWorkflowExecutionsResponse
	var err error
	for result == nil || (len(result.Executions) == 0 && result.NextPageToken != nil) {
		// the executions will be empty if the query is still running before timeout
		// so keep calling the API until some results are returned (query completed)
		ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

		result, err = sdkClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			cancel()
			ErrorAndExit("Failed to list archived workflow.", err)
		}
		request.NextPageToken = result.NextPageToken
		cancel()
	}

	var table *tablewriter.Table
	var printFn func([]*workflowpb.WorkflowExecutionInfo, bool)
	var prePrintFn func()
	var postPrintFn func()
	printRawTime := c.Bool(FlagPrintRawTime)
	printDateTime := c.Bool(FlagPrintDateTime)
	printMemo := c.Bool(FlagPrintMemo)
	printSearchAttr := c.Bool(FlagPrintSearchAttr)
	if printJSON || printDecodedRaw {
		prePrintFn = func() { fmt.Println("[") }
		printFn = func(execution []*workflowpb.WorkflowExecutionInfo, more bool) {
			printListResults(execution, printJSON, more)
		}
		postPrintFn = func() { fmt.Println("]") }
	} else {
		table = createTableForListWorkflow(c, false, false)
		prePrintFn = func() { table.ClearRows() }
		printFn = func(execution []*workflowpb.WorkflowExecutionInfo, _ bool) {
			appendWorkflowExecutionsToTable(
				table,
				execution,
				false,
				printRawTime,
				printDateTime,
				printMemo,
				printSearchAttr,
			)
		}
		postPrintFn = func() { table.Render() }
	}

	prePrintFn()
	printFn(result.Executions, result.NextPageToken != nil)
	for len(result.NextPageToken) != 0 {
		if !printAll {
			postPrintFn()
		}

		if !printAll && !showNextPage() {
			break
		}

		request.NextPageToken = result.NextPageToken
		// create a new context for each new request as each request may take a long time
		ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
		result, err = sdkClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			cancel()
			ErrorAndExit("Failed to list archived workflow", err)
		}
		cancel()

		if !printAll {
			prePrintFn()
		}
		printFn(result.Executions, result.NextPageToken != nil)
	}

	// if next page token is not nil here, then it means we are not in all mode,
	// and user doesn't want to view the next page. In that case the post
	// operation has already been done and we don't want to perform it again.
	if len(result.NextPageToken) == 0 {
		postPrintFn()
	}
}

// DescribeWorkflow show information about the specified workflow execution
func DescribeWorkflow(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	describeWorkflowHelper(c, wid, rid)
}

// DescribeWorkflowWithID show information about the specified workflow execution
func DescribeWorkflowWithID(c *cli.Context) {
	if !c.Args().Present() {
		ErrorAndExit("Argument workflow_id is required.", nil)
	}
	wid := c.Args().First()
	rid := ""
	if c.NArg() >= 2 {
		rid = c.Args().Get(1)
	}

	describeWorkflowHelper(c, wid, rid)
}

func describeWorkflowHelper(c *cli.Context, wid, rid string) {
	frontendClient := cFactory.FrontendClient(c)
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	printRaw := c.Bool(FlagPrintRaw) // printRaw is false by default,
	// and will show datetime and decoded search attributes instead of raw timestamp and byte arrays
	printResetPointsOnly := c.Bool(FlagResetPointsOnly)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		ErrorAndExit("Describe workflow execution failed", err)
	}

	if printResetPointsOnly {
		printAutoResetPoints(resp)
		return
	}

	if printRaw {
		prettyPrintJSONObject(resp)
	} else {
		prettyPrintJSONObject(convertDescribeWorkflowExecutionResponse(resp))
	}
}

func printAutoResetPoints(resp *workflowservice.DescribeWorkflowExecutionResponse) {
	fmt.Println("Auto Reset Points:")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(true)
	table.SetColumnSeparator("|")
	header := []string{"Binary Checksum", "Create Time", "RunId", "EventId"}
	headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
	table.SetHeader(header)
	table.SetHeaderColor(headerColor...)
	if resp.WorkflowExecutionInfo.AutoResetPoints != nil && len(resp.WorkflowExecutionInfo.AutoResetPoints.Points) > 0 {
		for _, pt := range resp.WorkflowExecutionInfo.AutoResetPoints.Points {
			var row []string
			row = append(row, pt.GetBinaryChecksum())
			row = append(row, timestamp.TimeValue(pt.GetCreateTime()).String())
			row = append(row, pt.GetRunId())
			row = append(row, convert.Int64ToString(pt.GetFirstWorkflowTaskCompletedId()))
			table.Append(row)
		}
	}
	table.Render()
}

func convertDescribeWorkflowExecutionResponse(resp *workflowservice.DescribeWorkflowExecutionResponse) *clispb.DescribeWorkflowExecutionResponse {

	info := resp.GetWorkflowExecutionInfo()
	executionInfo := &clispb.WorkflowExecutionInfo{
		Execution:            info.GetExecution(),
		Type:                 info.GetType(),
		CloseTime:            info.GetCloseTime(),
		StartTime:            info.GetStartTime(),
		Status:               info.GetStatus(),
		HistoryLength:        info.GetHistoryLength(),
		ParentNamespaceId:    info.GetParentNamespaceId(),
		ParentExecution:      info.GetParentExecution(),
		Memo:                 info.GetMemo(),
		SearchAttributes:     convertSearchAttributes(info.GetSearchAttributes()),
		AutoResetPoints:      info.GetAutoResetPoints(),
		StateTransitionCount: info.GetStateTransitionCount(),
	}

	var pendingActivitiesStr []*clispb.PendingActivityInfo
	for _, pendingActivity := range resp.GetPendingActivities() {
		pendingActivityStr := &clispb.PendingActivityInfo{
			ActivityId:         pendingActivity.GetActivityId(),
			ActivityType:       pendingActivity.GetActivityType(),
			State:              pendingActivity.GetState(),
			ScheduledTime:      pendingActivity.GetScheduledTime(),
			LastStartedTime:    pendingActivity.GetLastStartedTime(),
			LastHeartbeatTime:  pendingActivity.GetLastHeartbeatTime(),
			Attempt:            pendingActivity.GetAttempt(),
			MaximumAttempts:    pendingActivity.GetMaximumAttempts(),
			ExpirationTime:     pendingActivity.GetExpirationTime(),
			LastFailure:        convertFailure(pendingActivity.GetLastFailure()),
			LastWorkerIdentity: pendingActivity.GetLastWorkerIdentity(),
		}

		if pendingActivity.GetHeartbeatDetails() != nil {
			pendingActivityStr.HeartbeatDetails = payloads.ToString(pendingActivity.GetHeartbeatDetails())
		}
		pendingActivitiesStr = append(pendingActivitiesStr, pendingActivityStr)
	}

	return &clispb.DescribeWorkflowExecutionResponse{
		ExecutionConfig:       resp.ExecutionConfig,
		WorkflowExecutionInfo: executionInfo,
		PendingActivities:     pendingActivitiesStr,
		PendingChildren:       resp.PendingChildren,
	}
}

func convertSearchAttributes(searchAttributes *commonpb.SearchAttributes) *clispb.SearchAttributes {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil
	}

	fields, err := searchattribute.Stringify(searchAttributes, nil)
	if err != nil {
		fmt.Printf("%s: unable to stringify search attribute: %v\n",
			colorMagenta("Warning"),
			err)
	}

	return &clispb.SearchAttributes{IndexedFields: fields}
}

func convertFailure(failure *failurepb.Failure) *clispb.Failure {
	if failure == nil {
		return nil
	}

	fType := reflect.TypeOf(failure.GetFailureInfo()).Elem().Name()
	if failure.GetTimeoutFailureInfo() != nil {
		fType = fmt.Sprintf("%s: %s", fType, failure.GetTimeoutFailureInfo().GetTimeoutType().String())
	}

	f := &clispb.Failure{
		Message:     failure.GetMessage(),
		Source:      failure.GetSource(),
		StackTrace:  failure.GetStackTrace(),
		Cause:       convertFailure(failure.GetCause()),
		FailureType: fType,
	}

	return f
}

func createTableForListWorkflow(c *cli.Context, listAll bool, queryOpen bool) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	header := []string{"Workflow Type", "Workflow Id", "Run Id", "Task Queue", "Start Time", "Execution Time"}
	headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
	if !queryOpen {
		header = append(header, "End Time")
		headerColor = append(headerColor, tableHeaderBlue)
	}
	if printMemo := c.Bool(FlagPrintMemo); printMemo {
		header = append(header, "Memo")
		headerColor = append(headerColor, tableHeaderBlue)
	}
	if printSearchAttr := c.Bool(FlagPrintSearchAttr); printSearchAttr {
		header = append(header, "Search Attributes")
		headerColor = append(headerColor, tableHeaderBlue)
	}
	table.SetHeader(header)
	if !listAll { // color is only friendly to ANSI terminal
		table.SetHeaderColor(headerColor...)
	}
	table.SetHeaderLine(false)
	return table
}

func listWorkflow(c *cli.Context, table *tablewriter.Table, queryOpen bool) func([]byte) ([]byte, int) {
	sdkClient := getSDKClient(c)

	earliestTime := parseTime(c.String(FlagEarliestTime), time.Time{}, time.Now().UTC())
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UTC(), time.Now().UTC())
	workflowID := c.String(FlagWorkflowID)
	workflowType := c.String(FlagWorkflowType)
	printRawTime := c.Bool(FlagPrintRawTime)
	printDateTime := c.Bool(FlagPrintDateTime)
	printMemo := c.Bool(FlagPrintMemo)
	printSearchAttr := c.Bool(FlagPrintSearchAttr)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForList
	}

	var workflowStatus enumspb.WorkflowExecutionStatus
	if c.IsSet(FlagWorkflowStatus) {
		if queryOpen {
			ErrorAndExit(optionErr, errors.New("you can only filter on status for closed workflow, not open workflow"))
		}
		workflowStatus = getWorkflowStatus(c.String(FlagWorkflowStatus))
	} else {
		workflowStatus = workflowStatusNotSet
	}

	if len(workflowID) > 0 && len(workflowType) > 0 {
		ErrorAndExit(optionErr, errors.New("you can filter on workflow_id or workflow_type, but not on both"))
	}

	prepareTable := func(next []byte) ([]byte, int) {
		var result []*workflowpb.WorkflowExecutionInfo
		var nextPageToken []byte
		if c.IsSet(FlagListQuery) {
			listQuery := c.String(FlagListQuery)
			result, nextPageToken = listWorkflowExecutions(sdkClient, pageSize, next, listQuery, c)
		} else if queryOpen {
			result, nextPageToken = listOpenWorkflow(sdkClient, pageSize, earliestTime, latestTime, workflowID, workflowType, next, c)
		} else {
			result, nextPageToken = listClosedWorkflow(sdkClient, pageSize, earliestTime, latestTime, workflowID, workflowType, workflowStatus, next, c)
		}

		appendWorkflowExecutionsToTable(
			table,
			result,
			queryOpen,
			printRawTime,
			printDateTime,
			printMemo,
			printSearchAttr,
		)

		return nextPageToken, len(result)
	}
	return prepareTable
}

func appendWorkflowExecutionsToTable(
	table *tablewriter.Table,
	executions []*workflowpb.WorkflowExecutionInfo,
	queryOpen bool,
	printRawTime bool,
	printDateTime bool,
	printMemo bool,
	printSearchAttr bool,
) {
	for _, e := range executions {
		var startTime, executionTime, closeTime string
		if printRawTime {
			startTime = fmt.Sprintf("%v", timestamp.TimeValue(e.GetStartTime()))
			executionTime = fmt.Sprintf("%v", e.GetExecutionTime())
			closeTime = fmt.Sprintf("%v", timestamp.TimeValue(e.GetCloseTime()))
		} else {
			startTime = formatTime(timestamp.TimeValue(e.GetStartTime()), !printDateTime)
			executionTime = formatTime(timestamp.TimeValue(e.GetExecutionTime()), !printDateTime)
			closeTime = formatTime(timestamp.TimeValue(e.GetCloseTime()), !printDateTime)
		}
		row := []string{trimWorkflowType(e.Type.GetName()), e.Execution.GetWorkflowId(), e.Execution.GetRunId(), e.GetTaskQueue(), startTime, executionTime}
		if !queryOpen {
			row = append(row, closeTime)
		}
		if printMemo {
			row = append(row, getPrintableMemo(e.Memo))
		}
		if printSearchAttr {
			row = append(row, getPrintableSearchAttributes(e.SearchAttributes))
		}
		table.Append(row)
	}
}

func printRunStatus(event *historypb.HistoryEvent) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		fmt.Printf("  Status: %s\n", colorGreen("COMPLETED"))
		result := payloads.ToString(event.GetWorkflowExecutionCompletedEventAttributes().GetResult())
		fmt.Printf("  Output: %s\n", result)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		fmt.Printf("  Status: %s\n", colorRed("FAILED"))
		fmt.Printf("  Failure: %s\n", convertFailure(event.GetWorkflowExecutionFailedEventAttributes().GetFailure()).String())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		fmt.Printf("  Status: %s\n", colorRed("TIMEOUT"))
		fmt.Printf("  Retry status: %s\n", event.GetWorkflowExecutionTimedOutEventAttributes().GetRetryState())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		fmt.Printf("  Status: %s\n", colorRed("CANCELED"))
		details := payloads.ToString(event.GetWorkflowExecutionCanceledEventAttributes().GetDetails())
		fmt.Printf("  Detail: %s\n", details)
	}
}

// in case workflow type is too long to show in table, trim it like .../example.Workflow
func trimWorkflowType(str string) string {
	res := str
	if len(str) >= maxWorkflowTypeLength {
		items := strings.Split(str, "/")
		res = items[len(items)-1]
		if len(res) >= maxWorkflowTypeLength {
			res = "..." + res[len(res)-maxWorkflowTypeLength:]
		} else {
			res = ".../" + res
		}
	}
	return res
}

func listWorkflowExecutions(sdkClient sdkclient.Client, pageSize int, nextPageToken []byte, query string, c *cli.Context) (
	[]*workflowpb.WorkflowExecutionInfo, []byte) {

	request := &workflowservice.ListWorkflowExecutionsRequest{
		PageSize:      int32(pageSize),
		NextPageToken: nextPageToken,
		Query:         query,
	}
	var workflows *workflowservice.ListWorkflowExecutionsResponse
	op := func() error {
		ctx, cancel := newContext(c)
		defer cancel()
		response, err := sdkClient.ListWorkflow(ctx, request)
		if err != nil {
			return err
		}
		workflows = response
		return nil
	}
	err := backoff.Retry(op, common.CreateFrontendServiceRetryPolicy(), common.IsContextDeadlineExceededErr)
	if err != nil {
		ErrorAndExit("Failed to list workflow.", err)
	}
	return workflows.Executions, workflows.NextPageToken
}

func listOpenWorkflow(sdkClient sdkclient.Client, pageSize int, earliestTime, latestTime time.Time, workflowID, workflowType string,
	nextPageToken []byte, c *cli.Context) ([]*workflowpb.WorkflowExecutionInfo, []byte) {

	request := &workflowservice.ListOpenWorkflowExecutionsRequest{
		MaximumPageSize: int32(pageSize),
		NextPageToken:   nextPageToken,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: &earliestTime,
			LatestTime:   &latestTime,
		},
	}
	if len(workflowID) > 0 {
		request.Filters = &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{WorkflowId: workflowID}}

	}
	if len(workflowType) > 0 {
		request.Filters = &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{TypeFilter: &filterpb.WorkflowTypeFilter{Name: workflowType}}
	}
	var workflows *workflowservice.ListOpenWorkflowExecutionsResponse
	op := func() error {
		ctx, cancel := newContext(c)
		defer cancel()
		response, err := sdkClient.ListOpenWorkflow(ctx, request)
		if err != nil {
			return err
		}
		workflows = response
		return nil
	}
	err := backoff.Retry(op, common.CreateFrontendServiceRetryPolicy(), common.IsContextDeadlineExceededErr)
	if err != nil {
		ErrorAndExit("Failed to list open workflow.", err)
	}
	return workflows.Executions, workflows.NextPageToken
}

func listClosedWorkflow(sdkClient sdkclient.Client, pageSize int, earliestTime, latestTime time.Time, workflowID, workflowType string,
	workflowStatus enumspb.WorkflowExecutionStatus, nextPageToken []byte, c *cli.Context) ([]*workflowpb.WorkflowExecutionInfo, []byte) {

	request := &workflowservice.ListClosedWorkflowExecutionsRequest{
		MaximumPageSize: int32(pageSize),
		NextPageToken:   nextPageToken,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: &earliestTime,
			LatestTime:   &latestTime,
		},
	}
	if len(workflowID) > 0 {
		request.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{WorkflowId: workflowID}}
	}
	if len(workflowType) > 0 {
		request.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_TypeFilter{TypeFilter: &filterpb.WorkflowTypeFilter{Name: workflowType}}
	}
	if workflowStatus != workflowStatusNotSet {
		request.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_StatusFilter{StatusFilter: &filterpb.StatusFilter{Status: workflowStatus}}
	}

	var workflows *workflowservice.ListClosedWorkflowExecutionsResponse
	op := func() error {
		ctx, cancel := newContext(c)
		defer cancel()
		response, err := sdkClient.ListClosedWorkflow(ctx, request)
		if err != nil {
			return err
		}
		workflows = response
		return nil
	}
	err := backoff.Retry(op, common.CreateFrontendServiceRetryPolicy(), common.IsContextDeadlineExceededErr)
	if err != nil {
		ErrorAndExit("Failed to list closed workflow.", err)
	}
	return workflows.Executions, workflows.NextPageToken
}

func getListResultInRaw(c *cli.Context, queryOpen bool, nextPageToken []byte) ([]*workflowpb.WorkflowExecutionInfo, []byte) {
	sdkClient := getSDKClient(c)
	earliestTime := parseTime(c.String(FlagEarliestTime), time.Time{}, time.Now().UTC())
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UTC(), time.Now().UTC())
	workflowID := c.String(FlagWorkflowID)
	workflowType := c.String(FlagWorkflowType)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForList
	}

	var workflowStatus enumspb.WorkflowExecutionStatus
	if c.IsSet(FlagWorkflowStatus) {
		if queryOpen {
			ErrorAndExit(optionErr, errors.New("you can only filter on status for closed workflow, not open workflow"))
		}
		workflowStatus = getWorkflowStatus(c.String(FlagWorkflowStatus))
	} else {
		workflowStatus = workflowStatusNotSet
	}

	if len(workflowID) > 0 && len(workflowType) > 0 {
		ErrorAndExit(optionErr, errors.New("you can filter on workflow_id or workflow_type, but not on both"))
	}

	var result []*workflowpb.WorkflowExecutionInfo
	if c.IsSet(FlagListQuery) {
		listQuery := c.String(FlagListQuery)
		result, nextPageToken = listWorkflowExecutions(sdkClient, pageSize, nextPageToken, listQuery, c)
	} else if queryOpen {
		result, nextPageToken = listOpenWorkflow(sdkClient, pageSize, earliestTime, latestTime, workflowID, workflowType, nextPageToken, c)
	} else {
		result, nextPageToken = listClosedWorkflow(sdkClient, pageSize, earliestTime, latestTime, workflowID, workflowType, workflowStatus, nextPageToken, c)
	}

	return result, nextPageToken
}

func getScanResultInRaw(c *cli.Context, nextPageToken []byte) ([]*workflowpb.WorkflowExecutionInfo, []byte) {
	sdkClient := getSDKClient(c)
	listQuery := c.String(FlagListQuery)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForScan
	}

	return scanWorkflowExecutions(sdkClient, pageSize, nextPageToken, listQuery, c)
}

func scanWorkflowExecutions(sdkClient sdkclient.Client, pageSize int, nextPageToken []byte, query string, c *cli.Context) ([]*workflowpb.WorkflowExecutionInfo, []byte) {

	request := &workflowservice.ScanWorkflowExecutionsRequest{
		PageSize:      int32(pageSize),
		NextPageToken: nextPageToken,
		Query:         query,
	}

	var workflows *workflowservice.ScanWorkflowExecutionsResponse
	op := func() error {
		ctx, cancel := newContext(c)
		defer cancel()
		response, err := sdkClient.ScanWorkflow(ctx, request)
		if err != nil {
			return err
		}
		workflows = response
		return nil
	}
	err := backoff.Retry(op, common.CreateFrontendServiceRetryPolicy(), common.IsContextDeadlineExceededErr)
	if err != nil {
		ErrorAndExit("Failed to list workflow.", err)
	}
	return workflows.Executions, workflows.NextPageToken
}

func scanWorkflow(c *cli.Context, table *tablewriter.Table, queryOpen bool) func([]byte) ([]byte, int) {
	sdkClient := getSDKClient(c)

	printRawTime := c.Bool(FlagPrintRawTime)
	printDateTime := c.Bool(FlagPrintDateTime)
	printMemo := c.Bool(FlagPrintMemo)
	printSearchAttr := c.Bool(FlagPrintSearchAttr)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForScan
	}

	prepareTable := func(next []byte) ([]byte, int) {
		var result []*workflowpb.WorkflowExecutionInfo
		var nextPageToken []byte
		listQuery := c.String(FlagListQuery)
		result, nextPageToken = scanWorkflowExecutions(sdkClient, pageSize, next, listQuery, c)

		for _, e := range result {
			var startTime, executionTime, closeTime string
			if printRawTime {
				startTime = fmt.Sprintf("%v", timestamp.TimeValue(e.GetStartTime()))
				executionTime = fmt.Sprintf("%v", e.GetExecutionTime())
				closeTime = fmt.Sprintf("%v", timestamp.TimeValue(e.GetCloseTime()))
			} else {
				startTime = formatTime(timestamp.TimeValue(e.GetStartTime()), !printDateTime)
				executionTime = formatTime(timestamp.TimeValue(e.GetExecutionTime()), !printDateTime)
				closeTime = formatTime(timestamp.TimeValue(e.GetCloseTime()), !printDateTime)
			}
			row := []string{trimWorkflowType(e.Type.GetName()), e.Execution.GetWorkflowId(), e.Execution.GetRunId(), startTime, executionTime}
			if !queryOpen {
				row = append(row, closeTime)
			}
			if printMemo {
				row = append(row, getPrintableMemo(e.Memo))
			}
			if printSearchAttr {
				row = append(row, getPrintableSearchAttributes(e.SearchAttributes))
			}
			table.Append(row)
		}

		return nextPageToken, len(result)
	}
	return prepareTable
}
func getWorkflowStatus(statusStr string) enumspb.WorkflowExecutionStatus {
	if status, ok := workflowClosedStatusMap[strings.ToLower(statusStr)]; ok {
		return status
	}
	ErrorAndExit(optionErr, errors.New("option status is not one of allowed values "+
		"[running, completed, failed, canceled, terminated, continueasnew, timedout]"))
	return 0
}

// default will print decoded raw
func printListResults(executions []*workflowpb.WorkflowExecutionInfo, inJSON bool, more bool) {
	encoder := codec.NewJSONPBEncoder()
	for i, execution := range executions {
		if inJSON {
			j, _ := encoder.Encode(execution)
			if more || i < len(executions)-1 {
				fmt.Println(string(j) + ",")
			} else {
				fmt.Println(string(j))
			}
		} else {
			if more || i < len(executions)-1 {
				fmt.Println(stringify.AnyToString(execution, true, 0, dataconverter.GetCurrent()) + ",")
			} else {
				fmt.Println(stringify.AnyToString(execution, true, 0, dataconverter.GetCurrent()))
			}
		}
	}
}

// ObserveHistory show the process of running workflow
func ObserveHistory(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	printWorkflowProgress(c, wid, rid)
}

// ResetWorkflow reset workflow
func ResetWorkflow(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	reason := getRequiredOption(c, FlagReason)
	if len(reason) == 0 {
		ErrorAndExit("wrong reason", fmt.Errorf("reason cannot be empty"))
	}
	rid := c.String(FlagRunID)
	eventID := c.Int64(FlagEventID)
	resetType := c.String(FlagResetType)
	extraForResetType, ok := resetTypesMap[resetType]
	if !ok && eventID <= 0 {
		ErrorAndExit(fmt.Sprintf("must specify either valid event_id or reset_type (one of %s)", strings.Join(mapKeysToArray(resetTypesMap), ", ")), nil)
	}
	if ok && len(extraForResetType.(string)) > 0 {
		getRequiredOption(c, extraForResetType.(string))
	}
	resetReapplyType := c.String(FlagResetReapplyType)
	if _, ok := resetReapplyTypesMap[resetReapplyType]; !ok {
		ErrorAndExit(fmt.Sprintf("must specify valid reset reapply type: %v", strings.Join(mapKeysToArray(resetReapplyTypesMap), ", ")), nil)
	}

	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.FrontendClient(c)

	resetBaseRunID := rid
	workflowTaskFinishID := eventID
	var err error
	if resetType != "" {
		resetBaseRunID, workflowTaskFinishID, err = getResetEventIDByType(ctx, c, resetType, namespace, wid, rid, frontendClient)
		if err != nil {
			ErrorAndExit("getResetEventIDByType failed", err)
		}
	}
	resp, err := frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      resetBaseRunID,
		},
		Reason:                    fmt.Sprintf("%v:%v", getCurrentUserFromEnv(), reason),
		WorkflowTaskFinishEventId: workflowTaskFinishID,
		RequestId:                 uuid.New(),
		ResetReapplyType:          resetReapplyTypesMap[resetReapplyType].(enumspb.ResetReapplyType),
	})
	if err != nil {
		ErrorAndExit("reset failed", err)
	}
	prettyPrintJSONObject(resp)
}

func processResets(c *cli.Context, namespace string, wes chan commonpb.WorkflowExecution, done chan bool, wg *sync.WaitGroup, params batchResetParamsType) {
	for {
		select {
		case we := <-wes:
			fmt.Println("received: ", we.GetWorkflowId(), we.GetRunId())
			wid := we.GetWorkflowId()
			rid := we.GetRunId()
			var err error
			for i := 0; i < 3; i++ {
				err = doReset(c, namespace, wid, rid, params)
				if err == nil {
					break
				}
				if _, ok := err.(*serviceerror.InvalidArgument); ok {
					break
				}
				fmt.Println("failed and retry...: ", wid, rid, err)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000)))
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
			if err != nil {
				fmt.Println("[ERROR] failed processing: ", wid, rid, err.Error())
			}
		case <-done:
			wg.Done()
			return
		}
	}
}

type batchResetParamsType struct {
	reason               string
	skipOpen             bool
	nonDeterministicOnly bool
	skipBaseNotCurrent   bool
	dryRun               bool
	resetType            string
}

// ResetInBatch resets workflow in batch
func ResetInBatch(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	resetType := getRequiredOption(c, FlagResetType)

	inFileName := c.String(FlagInputFile)
	query := c.String(FlagListQuery)
	excFileName := c.String(FlagExcludeFile)
	separator := c.String(FlagInputSeparator)
	parallel := c.Int(FlagParallism)

	extraForResetType, ok := resetTypesMap[resetType]
	if !ok {
		ErrorAndExit("Not supported reset type", nil)
	} else if len(extraForResetType.(string)) > 0 {
		getRequiredOption(c, extraForResetType.(string))
	}

	batchResetParams := batchResetParamsType{
		reason:               getRequiredOption(c, FlagReason),
		skipOpen:             c.Bool(FlagSkipCurrentOpen),
		nonDeterministicOnly: c.Bool(FlagNonDeterministicOnly),
		skipBaseNotCurrent:   c.Bool(FlagSkipBaseIsNotCurrent),
		dryRun:               c.Bool(FlagDryRun),
		resetType:            resetType,
	}

	if inFileName == "" && query == "" {
		ErrorAndExit("Must provide input file or list query to get target workflows to reset", nil)
	}

	wg := &sync.WaitGroup{}

	wes := make(chan commonpb.WorkflowExecution)
	done := make(chan bool)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go processResets(c, namespace, wes, done, wg, batchResetParams)
	}

	// read exclude
	excludes := map[string]string{}
	if len(excFileName) > 0 {
		// This code is only used in the CLI. The input provided is from a trusted user.
		// #nosec
		excFile, err := os.Open(excFileName)
		if err != nil {
			ErrorAndExit("Open failed2", err)
		}
		defer excFile.Close()
		scanner := bufio.NewScanner(excFile)
		idx := 0
		for scanner.Scan() {
			idx++
			line := strings.TrimSpace(scanner.Text())
			if len(line) == 0 {
				fmt.Printf("line %v is empty, skipped\n", idx)
				continue
			}
			cols := strings.Split(line, separator)
			if len(cols) < 1 {
				ErrorAndExit("Split failed", fmt.Errorf("line %v has less than 1 cols separated by comma, only %v ", idx, len(cols)))
			}
			wid := strings.TrimSpace(cols[0])
			rid := "not-needed"
			excludes[wid] = rid
		}
	}
	fmt.Println("num of excludes:", len(excludes))

	if len(inFileName) > 0 {
		inFile, err := os.Open(inFileName)
		if err != nil {
			ErrorAndExit("Open failed", err)
		}
		defer inFile.Close()
		scanner := bufio.NewScanner(inFile)
		idx := 0
		for scanner.Scan() {
			idx++
			line := strings.TrimSpace(scanner.Text())
			if len(line) == 0 {
				fmt.Printf("line %v is empty, skipped\n", idx)
				continue
			}
			cols := strings.Split(line, separator)
			if len(cols) < 1 {
				ErrorAndExit("Split failed", fmt.Errorf("line %v has less than 1 cols separated by comma, only %v ", idx, len(cols)))
			}
			fmt.Printf("Start processing line %v ...\n", idx)
			wid := strings.TrimSpace(cols[0])
			rid := ""
			if len(cols) > 1 {
				rid = strings.TrimSpace(cols[1])
			}

			_, ok := excludes[wid]
			if ok {
				fmt.Println("skip by exclude file: ", wid, rid)
				continue
			}

			wes <- commonpb.WorkflowExecution{
				WorkflowId: wid,
				RunId:      rid,
			}
		}
	} else {
		sdkClient := getSDKClient(c)
		pageSize := 1000
		var nextPageToken []byte
		var result []*workflowpb.WorkflowExecutionInfo
		for {
			result, nextPageToken = scanWorkflowExecutions(sdkClient, pageSize, nextPageToken, query, c)
			for _, we := range result {
				wid := we.Execution.GetWorkflowId()
				rid := we.Execution.GetRunId()
				_, ok := excludes[wid]
				if ok {
					fmt.Println("skip by exclude file: ", wid, rid)
					continue
				}

				wes <- commonpb.WorkflowExecution{
					WorkflowId: wid,
					RunId:      rid,
				}
			}

			if nextPageToken == nil {
				break
			}
		}
	}

	close(done)
	fmt.Println("wait for all goroutines...")
	wg.Wait()
}

func printErrorAndReturn(msg string, err error) error {
	fmt.Println(msg)
	return err
}

func doReset(c *cli.Context, namespace, wid, rid string, params batchResetParamsType) error {
	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.FrontendClient(c)
	resp, err := frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
		},
	})
	if err != nil {
		return printErrorAndReturn("DescribeWorkflowExecution failed", err)
	}

	currentRunID := resp.WorkflowExecutionInfo.Execution.GetRunId()
	if currentRunID != rid && params.skipBaseNotCurrent {
		fmt.Println("skip because base run is different from current run: ", wid, rid, currentRunID)
		return nil
	}
	if rid == "" {
		rid = currentRunID
	}

	if resp.WorkflowExecutionInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || resp.WorkflowExecutionInfo.CloseTime == nil {
		if params.skipOpen {
			fmt.Println("skip because current run is open: ", wid, rid, currentRunID)
			// skip and not terminate current if open
			return nil
		}
	}

	if params.nonDeterministicOnly {
		isLDN, err := isLastEventWorkflowTaskFailedWithNonDeterminism(ctx, namespace, wid, rid, frontendClient)
		if err != nil {
			return printErrorAndReturn("check isLastEventWorkflowTaskFailedWithNonDeterminism failed", err)
		}
		if !isLDN {
			fmt.Println("skip because last event is not WorkflowTaskFailedWithNonDeterminism")
			return nil
		}
	}

	resetBaseRunID, workflowTaskFinishID, err := getResetEventIDByType(ctx, c, params.resetType, namespace, wid, rid, frontendClient)
	if err != nil {
		return printErrorAndReturn("getResetEventIDByType failed", err)
	}
	fmt.Println("WorkflowTaskFinishEventId for reset:", wid, rid, resetBaseRunID, workflowTaskFinishID)

	if params.dryRun {
		fmt.Printf("dry run to reset wid: %v, rid:%v to baseRunId:%v, eventId:%v \n", wid, rid, resetBaseRunID, workflowTaskFinishID)
	} else {
		resp2, err := frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
			Namespace: namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: wid,
				RunId:      resetBaseRunID,
			},
			WorkflowTaskFinishEventId: workflowTaskFinishID,
			RequestId:                 uuid.New(),
			Reason:                    fmt.Sprintf("%v:%v", getCurrentUserFromEnv(), params.reason),
		})

		if err != nil {
			return printErrorAndReturn("ResetWorkflowExecution failed", err)
		}
		fmt.Println("new runId for wid/rid is ,", wid, rid, resp2.GetRunId())
	}

	return nil
}

func isLastEventWorkflowTaskFailedWithNonDeterminism(ctx context.Context, namespace, wid, rid string, frontendClient workflowservice.WorkflowServiceClient) (bool, error) {
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}

	var firstEvent, workflowTaskFailedEvent *historypb.HistoryEvent
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return false, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if firstEvent == nil {
				firstEvent = e
			}
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
				workflowTaskFailedEvent = e
			} else if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				workflowTaskFailedEvent = nil
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}

	if workflowTaskFailedEvent != nil {
		attr := workflowTaskFailedEvent.GetWorkflowTaskFailedEventAttributes()

		if attr.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE ||
			strings.Contains(attr.GetFailure().GetMessage(), "nondeterministic") {
			fmt.Printf("found non determnistic workflow wid:%v, rid:%v, orignalStartTime:%v \n", wid, rid, timestamp.TimeValue(firstEvent.GetEventTime()))
			return true, nil
		}
	}

	return false, nil
}

func getResetEventIDByType(ctx context.Context, c *cli.Context, resetType, namespace, wid, rid string, frontendClient workflowservice.WorkflowServiceClient) (resetBaseRunID string, workflowTaskFinishID int64, err error) {
	fmt.Println("resetType:", resetType)
	switch resetType {
	case "LastWorkflowTask":
		resetBaseRunID, workflowTaskFinishID, err = getLastWorkflowTaskEventID(ctx, namespace, wid, rid, frontendClient)
		if err != nil {
			return
		}
	case "LastContinuedAsNew":
		resetBaseRunID, workflowTaskFinishID, err = getLastContinueAsNewID(ctx, namespace, wid, rid, frontendClient)
		if err != nil {
			return
		}
	case "FirstWorkflowTask":
		resetBaseRunID, workflowTaskFinishID, err = getFirstWorkflowTaskEventID(ctx, namespace, wid, rid, frontendClient)
		if err != nil {
			return
		}
	case "BadBinary":
		binCheckSum := c.String(FlagResetBadBinaryChecksum)
		resetBaseRunID, workflowTaskFinishID, err = getBadWorkflowTaskCompletedID(ctx, namespace, wid, rid, binCheckSum, frontendClient)
		if err != nil {
			return
		}
	default:
		panic("not supported resetType")
	}
	return
}

// Returns event id of the last completed task or id of the next event after scheduled task.
func getLastWorkflowTaskEventID(ctx context.Context, namespace, wid, rid string, frontendClient workflowservice.WorkflowServiceClient) (resetBaseRunID string, workflowTaskEventID int64, err error) {
	resetBaseRunID = rid
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}

	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				workflowTaskEventID = e.GetEventId()
			} else if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				workflowTaskEventID = e.GetEventId() + 1
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}
	if workflowTaskEventID == 0 {
		return "", 0, printErrorAndReturn("Get LastWorkflowTaskID failed", fmt.Errorf("unable to find any scheduled or completed task"))
	}
	return
}

func badChecksum(bad string) func(string) error {
	return func(maybeBad string) error {
		if maybeBad == bad {
			return fmt.Errorf("Bad checksum %q", bad)
		}
		return nil
	}
}

func getBadWorkflowTaskCompletedID(ctx context.Context, namespace, wid, rid, binChecksum string, frontendClient workflowservice.WorkflowServiceClient) (resetBaseRunID string, workflowTaskCompletedID int64, err error) {
	resetBaseRunID = rid
	resp, err := frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		return "", 0, printErrorAndReturn("DescribeWorkflowExecution failed", err)
	}

	_, p := workflow.FindAutoResetPoint(clock.NewRealTimeSource(), badChecksum(binChecksum), resp.WorkflowExecutionInfo.AutoResetPoints)
	if p != nil {
		workflowTaskCompletedID = p.GetFirstWorkflowTaskCompletedId()
	}

	if workflowTaskCompletedID == 0 {
		return "", 0, printErrorAndReturn("Get BadWorkflowTaskCompletedID failed", serviceerror.NewInvalidArgument("no WorkflowTaskCompletedID"))
	}
	return
}

// Returns id of the first workflow task completed event or if it doesn't exist then id of the event after task scheduled event.
func getFirstWorkflowTaskEventID(ctx context.Context, namespace, wid, rid string, frontendClient workflowservice.WorkflowServiceClient) (resetBaseRunID string, workflowTaskEventID int64, err error) {
	resetBaseRunID = rid
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				workflowTaskEventID = e.GetEventId()
				return resetBaseRunID, workflowTaskEventID, nil
			}
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
				if workflowTaskEventID == 0 {
					workflowTaskEventID = e.GetEventId() + 1
				}
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}
	if workflowTaskEventID == 0 {
		return "", 0, printErrorAndReturn("Get FirstWorkflowTaskID failed", fmt.Errorf("unable to find any scheduled or completed task"))
	}
	return
}

func getLastContinueAsNewID(ctx context.Context, namespace, wid, rid string, frontendClient workflowservice.WorkflowServiceClient) (resetBaseRunID string, workflowTaskCompletedID int64, err error) {
	// get first event
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		MaximumPageSize: 1,
		NextPageToken:   nil,
	}
	resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
	if err != nil {
		return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
	}
	firstEvent := resp.History.Events[0]
	resetBaseRunID = firstEvent.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId()
	if resetBaseRunID == "" {
		return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", fmt.Errorf("cannot get resetBaseRunId"))
	}

	req = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      resetBaseRunID,
		},
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				workflowTaskCompletedID = e.GetEventId()
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}
	if workflowTaskCompletedID == 0 {
		return "", 0, printErrorAndReturn("Get LastContinueAsNewID failed", fmt.Errorf("no WorkflowTaskCompletedID"))
	}
	return
}

// CompleteActivity completes an activity
func CompleteActivity(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	activityID := getRequiredOption(c, FlagActivityID)
	if len(activityID) == 0 {
		ErrorAndExit("Invalid activityId", fmt.Errorf("activityId cannot be empty"))
	}
	result := getRequiredOption(c, FlagResult)
	identity := getRequiredOption(c, FlagIdentity)
	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.FrontendClient(c)
	_, err := frontendClient.RespondActivityTaskCompletedById(ctx, &workflowservice.RespondActivityTaskCompletedByIdRequest{
		Namespace:  namespace,
		WorkflowId: wid,
		RunId:      rid,
		ActivityId: activityID,
		Result:     payloads.EncodeString(result),
		Identity:   identity,
	})
	if err != nil {
		ErrorAndExit("Completing activity failed", err)
	} else {
		fmt.Println("Complete activity successfully.")
	}
}

// FailActivity fails an activity
func FailActivity(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	activityID := getRequiredOption(c, FlagActivityID)
	if len(activityID) == 0 {
		ErrorAndExit("Invalid activityId", fmt.Errorf("activityId cannot be empty"))
	}
	reason := getRequiredOption(c, FlagReason)
	detail := getRequiredOption(c, FlagDetail)
	identity := getRequiredOption(c, FlagIdentity)
	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.FrontendClient(c)
	_, err := frontendClient.RespondActivityTaskFailedById(ctx, &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  namespace,
		WorkflowId: wid,
		RunId:      rid,
		ActivityId: activityID,
		Failure: &failurepb.Failure{
			Message: reason,
			Source:  "CLI",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable: true,
				Details:      payloads.EncodeString(detail),
			}},
		},
		Identity: identity,
	})
	if err != nil {
		ErrorAndExit("Failing activity failed", err)
	} else {
		fmt.Println("Fail activity successfully.")
	}
}

// ObserveHistoryWithID show the process of running workflow
func ObserveHistoryWithID(c *cli.Context) {
	if !c.Args().Present() {
		ErrorAndExit("Argument workflow_id is required.", nil)
	}
	wid := c.Args().First()
	rid := ""
	if c.NArg() >= 2 {
		rid = c.Args().Get(1)
	}

	printWorkflowProgress(c, wid, rid)
}
