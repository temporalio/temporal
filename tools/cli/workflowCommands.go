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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/urfave/cli"
	"github.com/valyala/fastjson"
	s "go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/service/history"
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
	wfClient := getWorkflowClient(c)

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
	history, err := GetHistory(ctx, wfClient, wid, rid)
	if err != nil {
		ErrorAndExit(fmt.Sprintf("Failed to get history on workflow id: %s, run id: %s.", wid, rid), err)
	}

	prevEvent := s.HistoryEvent{}
	if printFully { // dump everything
		for _, e := range history.Events {
			if resetPointsOnly {
				if prevEvent.GetEventType() != s.EventTypeDecisionTaskStarted {
					prevEvent = *e
					continue
				}
				prevEvent = *e
			}
			fmt.Println(anyToString(e, true, maxFieldLength))
		}
	} else if c.IsSet(FlagEventID) { // only dump that event
		eventID := c.Int(FlagEventID)
		if eventID <= 0 || eventID > len(history.Events) {
			ErrorAndExit("EventId out of range.", fmt.Errorf("number should be 1 - %d inclusive", len(history.Events)))
		}
		e := history.Events[eventID-1]
		fmt.Println(anyToString(e, true, 0))
	} else { // use table to pretty output, will trim long text
		table := tablewriter.NewWriter(os.Stdout)
		table.SetBorder(false)
		table.SetColumnSeparator("")
		for _, e := range history.Events {
			if resetPointsOnly {
				if prevEvent.GetEventType() != s.EventTypeDecisionTaskStarted {
					prevEvent = *e
					continue
				}
				prevEvent = *e
			}

			columns := []string{}
			columns = append(columns, strconv.FormatInt(e.GetEventId(), 10))

			if printRawTime {
				columns = append(columns, strconv.FormatInt(e.GetTimestamp(), 10))
			} else if printDateTime {
				columns = append(columns, convertTime(e.GetTimestamp(), false))
			}
			if printVersion {
				columns = append(columns, fmt.Sprintf("(Version: %v)", *e.Version))
			}

			columns = append(columns, ColorEvent(e), HistoryEventToString(e, false, maxFieldLength))
			table.Append(columns)
		}
		table.Render()
	}

	if outputFileName != "" {
		serializer := &JSONHistorySerializer{}
		data, err := serializer.Serialize(history)
		if err != nil {
			ErrorAndExit("Failed to serialize history data.", err)
		}
		if err := ioutil.WriteFile(outputFileName, data, 0777); err != nil {
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
	serviceClient := cFactory.ClientFrontendClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	taskList := getRequiredOption(c, FlagTaskList)
	workflowType := getRequiredOption(c, FlagWorkflowType)
	et := c.Int(FlagExecutionTimeout)
	if et == 0 {
		ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagExecutionTimeout), nil)
	}
	dt := c.Int(FlagDecisionTimeout)
	wid := c.String(FlagWorkflowID)
	if len(wid) == 0 {
		wid = uuid.New()
	}
	reusePolicy := defaultWorkflowIDReusePolicy.Ptr()
	if c.IsSet(FlagWorkflowIDReusePolicy) {
		reusePolicy = getWorkflowIDReusePolicy(c.Int(FlagWorkflowIDReusePolicy))
	}

	input := processJSONInput(c)
	startRequest := &s.StartWorkflowExecutionRequest{
		RequestId:  common.StringPtr(uuid.New()),
		Domain:     common.StringPtr(domain),
		WorkflowId: common.StringPtr(wid),
		WorkflowType: &s.WorkflowType{
			Name: common.StringPtr(workflowType),
		},
		TaskList: &s.TaskList{
			Name: common.StringPtr(taskList),
		},
		Input:                               []byte(input),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(int32(et)),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(int32(dt)),
		Identity:                            common.StringPtr(getCliIdentity()),
		WorkflowIdReusePolicy:               reusePolicy,
	}
	if c.IsSet(FlagCronSchedule) {
		startRequest.CronSchedule = common.StringPtr(c.String(FlagCronSchedule))
	}

	memoFields := processMemo(c)
	if len(memoFields) != 0 {
		startRequest.Memo = &s.Memo{Fields: memoFields}
	}

	searchAttrFields := processSearchAttr(c)
	if len(searchAttrFields) != 0 {
		startRequest.SearchAttributes = &s.SearchAttributes{IndexedFields: searchAttrFields}
	}

	startFn := func() {
		tcCtx, cancel := newContext(c)
		defer cancel()
		resp, err := serviceClient.StartWorkflowExecution(tcCtx, startRequest)

		if err != nil {
			ErrorAndExit("Failed to create workflow.", err)
		} else {
			fmt.Printf("Started Workflow Id: %s, run Id: %s\n", wid, resp.GetRunId())
		}
	}

	runFn := func() {
		tcCtx, cancel := newContextForLongPoll(c)
		defer cancel()
		resp, err := serviceClient.StartWorkflowExecution(tcCtx, startRequest)

		if err != nil {
			ErrorAndExit("Failed to run workflow.", err)
		}

		// print execution summary
		fmt.Println(colorMagenta("Running execution:"))
		table := tablewriter.NewWriter(os.Stdout)
		executionData := [][]string{
			{"Workflow Id", wid},
			{"Run Id", resp.GetRunId()},
			{"Type", workflowType},
			{"Domain", domain},
			{"Task List", taskList},
			{"Args", truncate(input)}, // in case of large input
		}
		table.SetBorder(false)
		table.SetColumnSeparator(":")
		table.AppendBulk(executionData) // Add Bulk Data
		table.Render()

		printWorkflowProgress(c, wid, resp.GetRunId())
	}

	if shouldPrintProgress {
		runFn()
	} else {
		startFn()
	}
}

func processSearchAttr(c *cli.Context) map[string][]byte {
	rawSearchAttrKey := c.String(FlagSearchAttributesKey)
	var searchAttrKeys []string
	if strings.TrimSpace(rawSearchAttrKey) != "" {
		searchAttrKeys = trimSpace(strings.Split(rawSearchAttrKey, searchAttrInputSeparator))
	}

	rawSearchAttrVal := c.String(FlagSearchAttributesVal)
	var searchAttrVals []interface{}
	if strings.TrimSpace(rawSearchAttrVal) != "" {
		searchAttrValsStr := trimSpace(strings.Split(rawSearchAttrVal, searchAttrInputSeparator))

		for _, v := range searchAttrValsStr {
			searchAttrVals = append(searchAttrVals, convertStringToRealType(v))
		}
	}

	if len(searchAttrKeys) != len(searchAttrVals) {
		ErrorAndExit("Number of search attributes keys and values are not equal.", nil)
	}

	fields := map[string][]byte{}
	for i, key := range searchAttrKeys {
		val, err := json.Marshal(searchAttrVals[i])
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Encode value %v error", val), err)
		}
		fields[key] = val
	}

	return fields
}

func processMemo(c *cli.Context) map[string][]byte {
	rawMemoKey := c.String(FlagMemoKey)
	var memoKeys []string
	if strings.TrimSpace(rawMemoKey) != "" {
		memoKeys = strings.Split(rawMemoKey, " ")
	}

	rawMemoValue := processJSONInputHelper(c, jsonTypeMemo)
	var memoValues []string

	var sc fastjson.Scanner
	sc.Init(rawMemoValue)
	for sc.Next() {
		memoValues = append(memoValues, sc.Value().String())
	}
	if err := sc.Error(); err != nil {
		ErrorAndExit("Parse json error.", err)
	}
	if len(memoKeys) != len(memoValues) {
		ErrorAndExit("Number of memo keys and values are not equal.", nil)
	}

	fields := map[string][]byte{}
	for i, key := range memoKeys {
		fields[key] = []byte(memoValues[i])
	}
	return fields
}

func getPrintableMemo(memo *s.Memo) string {
	buf := new(bytes.Buffer)
	for k, v := range memo.Fields {
		fmt.Fprintf(buf, "%s=%s\n", k, string(v))
	}
	return buf.String()
}

func getPrintableSearchAttr(searchAttr *s.SearchAttributes) string {
	buf := new(bytes.Buffer)
	for k, v := range searchAttr.IndexedFields {
		var decodedVal interface{}
		json.Unmarshal(v, &decodedVal)
		fmt.Fprintf(buf, "%s=%v\n", k, decodedVal)
	}
	return buf.String()
}

// helper function to print workflow progress with time refresh every second
func printWorkflowProgress(c *cli.Context, wid, rid string) {
	fmt.Println(colorMagenta("Progress:"))

	wfClient := getWorkflowClient(c)
	timeElapse := 1
	isTimeElapseExist := false
	doneChan := make(chan bool)
	var lastEvent *s.HistoryEvent // used for print result of this run
	ticker := time.NewTicker(time.Second).C

	tcCtx, cancel := newContextForLongPoll(c)
	defer cancel()

	showDetails := c.Bool(FlagShowDetail)
	var maxFieldLength int
	if c.IsSet(FlagMaxFieldLength) {
		maxFieldLength = c.Int(FlagMaxFieldLength)
	}

	go func() {
		iter := wfClient.GetWorkflowHistory(tcCtx, wid, rid, true, s.HistoryEventFilterTypeAllEvent)
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
				fmt.Printf("  %d, %s, %s, %s\n", event.GetEventId(), convertTime(event.GetTimestamp(), false), ColorEvent(event), HistoryEventToString(event, true, maxFieldLength))
			} else {
				fmt.Printf("  %d, %s, %s\n", event.GetEventId(), convertTime(event.GetTimestamp(), false), ColorEvent(event))
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
	wfClient := getWorkflowClient(c)

	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	reason := c.String(FlagReason)

	ctx, cancel := newContext(c)
	defer cancel()
	err := wfClient.TerminateWorkflow(ctx, wid, rid, reason, nil)

	if err != nil {
		ErrorAndExit("Terminate workflow failed.", err)
	} else {
		fmt.Println("Terminate workflow succeeded.")
	}
}

// CancelWorkflow cancels a workflow execution
func CancelWorkflow(c *cli.Context) {
	wfClient := getWorkflowClient(c)

	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()
	err := wfClient.CancelWorkflow(ctx, wid, rid)

	if err != nil {
		ErrorAndExit("Cancel workflow failed.", err)
	} else {
		fmt.Println("Cancel workflow succeeded.")
	}
}

// SignalWorkflow signals a workflow execution
func SignalWorkflow(c *cli.Context) {
	serviceClient := cFactory.ClientFrontendClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	name := getRequiredOption(c, FlagName)
	input := processJSONInput(c)

	tcCtx, cancel := newContext(c)
	defer cancel()
	err := serviceClient.SignalWorkflowExecution(tcCtx, &s.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		WorkflowExecution: &s.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      getPtrOrNilIfEmpty(rid),
		},
		SignalName: common.StringPtr(name),
		Input:      []byte(input),
		Identity:   common.StringPtr(getCliIdentity()),
	})

	if err != nil {
		ErrorAndExit("Signal workflow failed.", err)
	} else {
		fmt.Println("Signal workflow succeeded.")
	}
}

// QueryWorkflow query workflow execution
func QueryWorkflow(c *cli.Context) {
	getRequiredGlobalOption(c, FlagDomain) // for pre-check and alert if not provided
	getRequiredOption(c, FlagWorkflowID)
	queryType := getRequiredOption(c, FlagQueryType)

	queryWorkflowHelper(c, queryType)
}

// QueryWorkflowUsingStackTrace query workflow execution using __stack_trace as query type
func QueryWorkflowUsingStackTrace(c *cli.Context) {
	queryWorkflowHelper(c, "__stack_trace")
}

func queryWorkflowHelper(c *cli.Context, queryType string) {
	serviceClient := cFactory.ClientFrontendClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	input := processJSONInput(c)

	tcCtx, cancel := newContext(c)
	defer cancel()
	queryRequest := &s.QueryWorkflowRequest{
		Domain: common.StringPtr(domain),
		Execution: &s.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      getPtrOrNilIfEmpty(rid),
		},
		Query: &s.WorkflowQuery{
			QueryType: common.StringPtr(queryType),
		},
	}
	if input != "" {
		queryRequest.Query.QueryArgs = []byte(input)
	}
	if c.IsSet(FlagQueryRejectCondition) {
		var rejectCondition s.QueryRejectCondition
		switch c.String(FlagQueryRejectCondition) {
		case "not_open":
			rejectCondition = s.QueryRejectConditionNotOpen
		case "not_completed_cleanly":
			rejectCondition = s.QueryRejectConditionNotCompletedCleanly
		default:
			ErrorAndExit(fmt.Sprintf("invalid reject condition %v, valid values are \"not_open\" and \"not_completed_cleanly\"", c.String(FlagQueryRejectCondition)), nil)
		}
		queryRequest.QueryRejectCondition = &rejectCondition
	}
	if c.IsSet(FlagQueryConsistencyLevel) {
		var consistencyLevel s.QueryConsistencyLevel
		switch c.String(FlagQueryConsistencyLevel) {
		case "eventual":
			consistencyLevel = s.QueryConsistencyLevelEventual
		case "strong":
			consistencyLevel = s.QueryConsistencyLevelStrong
		default:
			ErrorAndExit(fmt.Sprintf("invalid query consistency level %v, valid values are \"eventual\" and \"strong\"", c.String(FlagQueryConsistencyLevel)), nil)
		}
		queryRequest.QueryConsistencyLevel = &consistencyLevel
	}
	queryResponse, err := serviceClient.QueryWorkflow(tcCtx, queryRequest)
	if err != nil {
		ErrorAndExit("Query workflow failed.", err)
		return
	}

	if queryResponse.QueryRejected != nil {
		fmt.Printf("Query was rejected, workflow is in state: %v\n", *queryResponse.QueryRejected.CloseStatus)
	} else {
		// assume it is json encoded
		fmt.Printf("Query result as JSON:\n%v\n", string(queryResponse.QueryResult))
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
		var results []*s.WorkflowExecutionInfo
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
		var results []*s.WorkflowExecutionInfo
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

	isQueryOpen := isQueryOpen(c.String(FlagListQuery))
	table := createTableForListWorkflow(c, true, isQueryOpen)
	prepareTable := scanWorkflow(c, table, isQueryOpen)
	var nextPageToken []byte
	for {
		nextPageToken, _ = prepareTable(nextPageToken)
		if len(nextPageToken) == 0 {
			break
		}
	}
	table.Render()
}

func isQueryOpen(query string) bool {
	var openWFPattern = regexp.MustCompile(`CloseTime[ ]*=[ ]*missing`)
	return openWFPattern.MatchString(query)
}

// CountWorkflow count number of workflows
func CountWorkflow(c *cli.Context) {
	wfClient := getWorkflowClient(c)

	query := c.String(FlagListQuery)
	request := &s.CountWorkflowExecutionsRequest{
		Query: common.StringPtr(query),
	}

	ctx, cancel := newContextForLongPoll(c)
	defer cancel()
	response, err := wfClient.CountWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to count workflow.", err)
	}

	fmt.Println(response.GetCount())
}

// ListArchivedWorkflow lists archived workflow executions based on filters
func ListArchivedWorkflow(c *cli.Context) {
	wfClient := getWorkflowClient(c)

	printJSON := c.Bool(FlagPrintJSON)
	printDecodedRaw := c.Bool(FlagPrintFullyDetail)
	pageSize := c.Int(FlagPageSize)
	listQuery := getRequiredOption(c, FlagListQuery)
	printAll := c.Bool(FlagAll)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForList
	}

	request := &s.ListArchivedWorkflowExecutionsRequest{
		PageSize: common.Int32Ptr(int32(pageSize)),
		Query:    common.StringPtr(listQuery),
	}

	contextTimeout := defaultContextTimeoutForListArchivedWorkflow
	if c.GlobalIsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}

	var result *s.ListArchivedWorkflowExecutionsResponse
	var err error
	for result == nil || (len(result.Executions) == 0 && result.NextPageToken != nil) {
		// the executions will be empty if the query is still running before timeout
		// so keep calling the API until some results are returned (query completed)
		ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)

		result, err = wfClient.ListArchivedWorkflow(ctx, request)
		if err != nil {
			cancel()
			ErrorAndExit("Failed to list archived workflow.", err)
		}
		request.NextPageToken = result.NextPageToken
		cancel()
	}

	var table *tablewriter.Table
	var printFn func([]*s.WorkflowExecutionInfo, bool)
	var prePrintFn func()
	var postPrintFn func()
	printRawTime := c.Bool(FlagPrintRawTime)
	printDateTime := c.Bool(FlagPrintDateTime)
	printMemo := c.Bool(FlagPrintMemo)
	printSearchAttr := c.Bool(FlagPrintSearchAttr)
	if printJSON || printDecodedRaw {
		prePrintFn = func() { fmt.Println("[") }
		printFn = func(execution []*s.WorkflowExecutionInfo, more bool) {
			printListResults(execution, printJSON, more)
		}
		postPrintFn = func() { fmt.Println("]") }
	} else {
		table = createTableForListWorkflow(c, false, false)
		prePrintFn = func() { table.ClearRows() }
		printFn = func(execution []*s.WorkflowExecutionInfo, _ bool) {
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
		result, err = wfClient.ListArchivedWorkflow(ctx, request)
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
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)
	printRaw := c.Bool(FlagPrintRaw) // printRaw is false by default,
	// and will show datetime and decoded search attributes instead of raw timestamp and byte arrays
	printResetPointsOnly := c.Bool(FlagResetPointsOnly)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := frontendClient.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
	})
	if err != nil {
		ErrorAndExit("Describe workflow execution failed", err)
	}

	if printResetPointsOnly {
		printAutoResetPoints(resp)
		return
	}

	var o interface{}
	if printRaw {
		o = resp
	} else {
		o = convertDescribeWorkflowExecutionResponse(resp, frontendClient, c)
	}

	prettyPrintJSONObject(o)
}

func printAutoResetPoints(resp *shared.DescribeWorkflowExecutionResponse) {
	fmt.Println("Auto Reset Points:")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(true)
	table.SetColumnSeparator("|")
	header := []string{"Binary Checksum", "Create Time", "RunID", "EventID"}
	headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
	table.SetHeader(header)
	table.SetHeaderColor(headerColor...)
	if resp.WorkflowExecutionInfo.AutoResetPoints != nil && len(resp.WorkflowExecutionInfo.AutoResetPoints.Points) > 0 {
		for _, pt := range resp.WorkflowExecutionInfo.AutoResetPoints.Points {
			var row []string
			row = append(row, pt.GetBinaryChecksum())
			row = append(row, time.Unix(0, pt.GetCreatedTimeNano()).String())
			row = append(row, pt.GetRunId())
			row = append(row, strconv.FormatInt(pt.GetFirstDecisionCompletedId(), 10))
			table.Append(row)
		}
	}
	table.Render()
}

// describeWorkflowExecutionResponse is used to print datetime instead of print raw time
type describeWorkflowExecutionResponse struct {
	ExecutionConfiguration *shared.WorkflowExecutionConfiguration
	WorkflowExecutionInfo  workflowExecutionInfo
	PendingActivities      []*pendingActivityInfo
	PendingChildren        []*shared.PendingChildExecutionInfo
}

// workflowExecutionInfo has same fields as shared.WorkflowExecutionInfo, but has datetime instead of raw time
type workflowExecutionInfo struct {
	Execution        *shared.WorkflowExecution
	Type             *shared.WorkflowType
	StartTime        *string // change from *int64
	CloseTime        *string // change from *int64
	CloseStatus      *shared.WorkflowExecutionCloseStatus
	HistoryLength    *int64
	ParentDomainID   *string
	ParentExecution  *shared.WorkflowExecution
	Memo             *shared.Memo
	SearchAttributes map[string]interface{}
	AutoResetPoints  *shared.ResetPoints
}

// pendingActivityInfo has same fields as shared.PendingActivityInfo, but different field type for better display
type pendingActivityInfo struct {
	ActivityID             *string
	ActivityType           *shared.ActivityType
	State                  *shared.PendingActivityState
	ScheduledTimestamp     *string `json:",omitempty"` // change from *int64
	LastStartedTimestamp   *string `json:",omitempty"` // change from *int64
	HeartbeatDetails       *string `json:",omitempty"` // change from []byte
	LastHeartbeatTimestamp *string `json:",omitempty"` // change from *int64
	Attempt                *int32  `json:",omitempty"`
	MaximumAttempts        *int32  `json:",omitempty"`
	ExpirationTimestamp    *string `json:",omitempty"` // change from *int64
	LastFailureReason      *string `json:",omitempty"`
	LastWorkerIdentity     *string `json:",omitempty"`
	LastFailureDetails     *string `json:",omitempty"` // change from []byte
}

func convertDescribeWorkflowExecutionResponse(resp *shared.DescribeWorkflowExecutionResponse,
	wfClient workflowserviceclient.Interface, c *cli.Context) *describeWorkflowExecutionResponse {

	info := resp.WorkflowExecutionInfo
	executionInfo := workflowExecutionInfo{
		Execution:        info.Execution,
		Type:             info.Type,
		StartTime:        common.StringPtr(convertTime(info.GetStartTime(), false)),
		CloseTime:        common.StringPtr(convertTime(info.GetCloseTime(), false)),
		CloseStatus:      info.CloseStatus,
		HistoryLength:    info.HistoryLength,
		ParentDomainID:   info.ParentDomainId,
		ParentExecution:  info.ParentExecution,
		Memo:             info.Memo,
		SearchAttributes: convertSearchAttributesToMapOfInterface(info.SearchAttributes, wfClient, c),
		AutoResetPoints:  info.AutoResetPoints,
	}

	var pendingActs []*pendingActivityInfo
	var tmpAct *pendingActivityInfo
	for _, pa := range resp.PendingActivities {
		tmpAct = &pendingActivityInfo{
			ActivityID:             pa.ActivityID,
			ActivityType:           pa.ActivityType,
			State:                  pa.State,
			ScheduledTimestamp:     timestampPtrToStringPtr(pa.ScheduledTimestamp, false),
			LastStartedTimestamp:   timestampPtrToStringPtr(pa.LastStartedTimestamp, false),
			LastHeartbeatTimestamp: timestampPtrToStringPtr(pa.LastHeartbeatTimestamp, false),
			Attempt:                pa.Attempt,
			MaximumAttempts:        pa.MaximumAttempts,
			ExpirationTimestamp:    timestampPtrToStringPtr(pa.ExpirationTimestamp, false),
			LastFailureReason:      pa.LastFailureReason,
			LastWorkerIdentity:     pa.LastWorkerIdentity,
		}
		if pa.HeartbeatDetails != nil {
			tmpAct.HeartbeatDetails = common.StringPtr(string(pa.HeartbeatDetails))
		}
		if pa.LastFailureDetails != nil {
			tmpAct.LastFailureDetails = common.StringPtr(string(pa.LastFailureDetails))
		}
		pendingActs = append(pendingActs, tmpAct)
	}

	return &describeWorkflowExecutionResponse{
		ExecutionConfiguration: resp.ExecutionConfiguration,
		WorkflowExecutionInfo:  executionInfo,
		PendingActivities:      pendingActs,
		PendingChildren:        resp.PendingChildren,
	}
}

func convertSearchAttributesToMapOfInterface(searchAttributes *shared.SearchAttributes,
	wfClient workflowserviceclient.Interface, c *cli.Context) map[string]interface{} {

	if searchAttributes == nil || len(searchAttributes.GetIndexedFields()) == 0 {
		return nil
	}

	result := make(map[string]interface{})
	ctx, cancel := newContext(c)
	defer cancel()
	validSearchAttributes, err := wfClient.GetSearchAttributes(ctx)
	if err != nil {
		ErrorAndExit("Error when get search attributes", err)
	}
	validKeys := validSearchAttributes.GetKeys()

	indexedFields := searchAttributes.GetIndexedFields()
	for k, v := range indexedFields {
		valueType := validKeys[k]
		switch valueType {
		case shared.IndexedValueTypeString, shared.IndexedValueTypeKeyword:
			var val string
			json.Unmarshal(v, &val)
			result[k] = val
		case shared.IndexedValueTypeInt:
			var val int64
			json.Unmarshal(v, &val)
			result[k] = val
		case shared.IndexedValueTypeDouble:
			var val float64
			json.Unmarshal(v, &val)
			result[k] = val
		case shared.IndexedValueTypeBool:
			var val bool
			json.Unmarshal(v, &val)
			result[k] = val
		case shared.IndexedValueTypeDatetime:
			var val time.Time
			json.Unmarshal(v, &val)
			result[k] = val
		default:
			ErrorAndExit(fmt.Sprintf("Error unknown index value type [%v]", valueType), nil)
		}
	}

	return result
}

func createTableForListWorkflow(c *cli.Context, listAll bool, queryOpen bool) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	header := []string{"Workflow Type", "Workflow ID", "Run ID", "Start Time", "Execution Time"}
	headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
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
	wfClient := getWorkflowClient(c)

	earliestTime := parseTime(c.String(FlagEarliestTime), 0)
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UnixNano())
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

	var workflowStatus s.WorkflowExecutionCloseStatus
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
		var result []*s.WorkflowExecutionInfo
		var nextPageToken []byte
		if c.IsSet(FlagListQuery) {
			listQuery := c.String(FlagListQuery)
			result, nextPageToken = listWorkflowExecutions(wfClient, pageSize, next, listQuery, c)
		} else if queryOpen {
			result, nextPageToken = listOpenWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, next, c)
		} else {
			result, nextPageToken = listClosedWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, workflowStatus, next, c)
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
	executions []*s.WorkflowExecutionInfo,
	queryOpen bool,
	printRawTime bool,
	printDateTime bool,
	printMemo bool,
	printSearchAttr bool,
) {
	for _, e := range executions {
		var startTime, executionTime, closeTime string
		if printRawTime {
			startTime = fmt.Sprintf("%d", e.GetStartTime())
			executionTime = fmt.Sprintf("%d", e.GetExecutionTime())
			closeTime = fmt.Sprintf("%d", e.GetCloseTime())
		} else {
			startTime = convertTime(e.GetStartTime(), !printDateTime)
			executionTime = convertTime(e.GetExecutionTime(), !printDateTime)
			closeTime = convertTime(e.GetCloseTime(), !printDateTime)
		}
		row := []string{trimWorkflowType(e.Type.GetName()), e.Execution.GetWorkflowId(), e.Execution.GetRunId(), startTime, executionTime}
		if !queryOpen {
			row = append(row, closeTime)
		}
		if printMemo {
			row = append(row, getPrintableMemo(e.Memo))
		}
		if printSearchAttr {
			row = append(row, getPrintableSearchAttr(e.SearchAttributes))
		}
		table.Append(row)
	}
}

func printRunStatus(event *s.HistoryEvent) {
	switch event.GetEventType() {
	case s.EventTypeWorkflowExecutionCompleted:
		fmt.Printf("  Status: %s\n", colorGreen("COMPLETED"))
		fmt.Printf("  Output: %s\n", string(event.WorkflowExecutionCompletedEventAttributes.Result))
	case s.EventTypeWorkflowExecutionFailed:
		fmt.Printf("  Status: %s\n", colorRed("FAILED"))
		fmt.Printf("  Reason: %s\n", event.WorkflowExecutionFailedEventAttributes.GetReason())
		fmt.Printf("  Detail: %s\n", string(event.WorkflowExecutionFailedEventAttributes.Details))
	case s.EventTypeWorkflowExecutionTimedOut:
		fmt.Printf("  Status: %s\n", colorRed("TIMEOUT"))
		fmt.Printf("  Timeout Type: %s\n", event.WorkflowExecutionTimedOutEventAttributes.GetTimeoutType())
	case s.EventTypeWorkflowExecutionCanceled:
		fmt.Printf("  Status: %s\n", colorRed("CANCELED"))
		fmt.Printf("  Detail: %s\n", string(event.WorkflowExecutionCanceledEventAttributes.Details))
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

func listWorkflowExecutions(client client.Client, pageSize int, nextPageToken []byte, query string, c *cli.Context) (
	[]*s.WorkflowExecutionInfo, []byte) {

	request := &s.ListWorkflowExecutionsRequest{
		PageSize:      common.Int32Ptr(int32(pageSize)),
		NextPageToken: nextPageToken,
		Query:         common.StringPtr(query),
	}

	ctx, cancel := newContextForLongPoll(c)
	defer cancel()
	response, err := client.ListWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to list workflow.", err)
	}
	return response.Executions, response.NextPageToken
}

func listOpenWorkflow(client client.Client, pageSize int, earliestTime, latestTime int64, workflowID, workflowType string,
	nextPageToken []byte, c *cli.Context) ([]*s.WorkflowExecutionInfo, []byte) {

	request := &s.ListOpenWorkflowExecutionsRequest{
		MaximumPageSize: common.Int32Ptr(int32(pageSize)),
		NextPageToken:   nextPageToken,
		StartTimeFilter: &s.StartTimeFilter{
			EarliestTime: common.Int64Ptr(earliestTime),
			LatestTime:   common.Int64Ptr(latestTime),
		},
	}
	if len(workflowID) > 0 {
		request.ExecutionFilter = &s.WorkflowExecutionFilter{WorkflowId: common.StringPtr(workflowID)}
	}
	if len(workflowType) > 0 {
		request.TypeFilter = &s.WorkflowTypeFilter{Name: common.StringPtr(workflowType)}
	}

	ctx, cancel := newContextForLongPoll(c)
	defer cancel()
	response, err := client.ListOpenWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to list open workflow.", err)
	}
	return response.Executions, response.NextPageToken
}

func listClosedWorkflow(client client.Client, pageSize int, earliestTime, latestTime int64, workflowID, workflowType string,
	workflowStatus s.WorkflowExecutionCloseStatus, nextPageToken []byte, c *cli.Context) ([]*s.WorkflowExecutionInfo, []byte) {

	request := &s.ListClosedWorkflowExecutionsRequest{
		MaximumPageSize: common.Int32Ptr(int32(pageSize)),
		NextPageToken:   nextPageToken,
		StartTimeFilter: &s.StartTimeFilter{
			EarliestTime: common.Int64Ptr(earliestTime),
			LatestTime:   common.Int64Ptr(latestTime),
		},
	}
	if len(workflowID) > 0 {
		request.ExecutionFilter = &s.WorkflowExecutionFilter{WorkflowId: common.StringPtr(workflowID)}
	}
	if len(workflowType) > 0 {
		request.TypeFilter = &s.WorkflowTypeFilter{Name: common.StringPtr(workflowType)}
	}
	if workflowStatus != workflowStatusNotSet {
		request.StatusFilter = &workflowStatus
	}

	ctx, cancel := newContextForLongPoll(c)
	defer cancel()
	response, err := client.ListClosedWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to list closed workflow.", err)
	}
	return response.Executions, response.NextPageToken
}

func getListResultInRaw(c *cli.Context, queryOpen bool, nextPageToken []byte) ([]*s.WorkflowExecutionInfo, []byte) {
	wfClient := getWorkflowClient(c)

	earliestTime := parseTime(c.String(FlagEarliestTime), 0)
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UnixNano())
	workflowID := c.String(FlagWorkflowID)
	workflowType := c.String(FlagWorkflowType)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForList
	}

	var workflowStatus s.WorkflowExecutionCloseStatus
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

	var result []*s.WorkflowExecutionInfo
	if c.IsSet(FlagListQuery) {
		listQuery := c.String(FlagListQuery)
		result, nextPageToken = listWorkflowExecutions(wfClient, pageSize, nextPageToken, listQuery, c)
	} else if queryOpen {
		result, nextPageToken = listOpenWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, nextPageToken, c)
	} else {
		result, nextPageToken = listClosedWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, workflowStatus, nextPageToken, c)
	}

	return result, nextPageToken
}

func getScanResultInRaw(c *cli.Context, nextPageToken []byte) ([]*s.WorkflowExecutionInfo, []byte) {
	wfClient := getWorkflowClient(c)
	listQuery := c.String(FlagListQuery)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForScan
	}

	return scanWorkflowExecutions(wfClient, pageSize, nextPageToken, listQuery, c)
}

func scanWorkflowExecutions(client client.Client, pageSize int, nextPageToken []byte, query string, c *cli.Context) ([]*s.WorkflowExecutionInfo, []byte) {

	request := &s.ListWorkflowExecutionsRequest{
		PageSize:      common.Int32Ptr(int32(pageSize)),
		NextPageToken: nextPageToken,
		Query:         common.StringPtr(query),
	}

	ctx, cancel := newContextForLongPoll(c)
	defer cancel()
	response, err := client.ScanWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to list workflow.", err)
	}
	return response.Executions, response.NextPageToken
}

func scanWorkflow(c *cli.Context, table *tablewriter.Table, queryOpen bool) func([]byte) ([]byte, int) {
	wfClient := getWorkflowClient(c)

	printRawTime := c.Bool(FlagPrintRawTime)
	printDateTime := c.Bool(FlagPrintDateTime)
	printMemo := c.Bool(FlagPrintMemo)
	printSearchAttr := c.Bool(FlagPrintSearchAttr)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForScan
	}

	prepareTable := func(next []byte) ([]byte, int) {
		var result []*s.WorkflowExecutionInfo
		var nextPageToken []byte
		listQuery := c.String(FlagListQuery)
		result, nextPageToken = scanWorkflowExecutions(wfClient, pageSize, next, listQuery, c)

		for _, e := range result {
			var startTime, executionTime, closeTime string
			if printRawTime {
				startTime = fmt.Sprintf("%d", e.GetStartTime())
				executionTime = fmt.Sprintf("%d", e.GetExecutionTime())
				closeTime = fmt.Sprintf("%d", e.GetCloseTime())
			} else {
				startTime = convertTime(e.GetStartTime(), !printDateTime)
				executionTime = convertTime(e.GetExecutionTime(), !printDateTime)
				closeTime = convertTime(e.GetCloseTime(), !printDateTime)
			}
			row := []string{trimWorkflowType(e.Type.GetName()), e.Execution.GetWorkflowId(), e.Execution.GetRunId(), startTime, executionTime}
			if !queryOpen {
				row = append(row, closeTime)
			}
			if printMemo {
				row = append(row, getPrintableMemo(e.Memo))
			}
			if printSearchAttr {
				row = append(row, getPrintableSearchAttr(e.SearchAttributes))
			}
			table.Append(row)
		}

		return nextPageToken, len(result)
	}
	return prepareTable
}
func getWorkflowStatus(statusStr string) s.WorkflowExecutionCloseStatus {
	if status, ok := workflowClosedStatusMap[strings.ToLower(statusStr)]; ok {
		return status
	}
	ErrorAndExit(optionErr, errors.New("option status is not one of allowed values "+
		"[completed, failed, canceled, terminated, continueasnew, timedout]"))
	return 0
}

func getWorkflowIDReusePolicy(value int) *s.WorkflowIdReusePolicy {
	if value >= 0 && value <= len(s.WorkflowIdReusePolicy_Values()) {
		return s.WorkflowIdReusePolicy(value).Ptr()
	}
	// At this point, the policy should return if the value is valid
	ErrorAndExit(fmt.Sprintf("Option %v value is not in supported range.", FlagWorkflowIDReusePolicy), nil)
	return nil
}

// default will print decoded raw
func printListResults(executions []*s.WorkflowExecutionInfo, inJSON bool, more bool) {
	for i, execution := range executions {
		if inJSON {
			j, _ := json.Marshal(execution)
			if more || i < len(executions)-1 {
				fmt.Println(string(j) + ",")
			} else {
				fmt.Println(string(j))
			}
		} else {
			if more || i < len(executions)-1 {
				fmt.Println(anyToString(execution, true, 0) + ",")
			} else {
				fmt.Println(anyToString(execution, true, 0))
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
	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	reason := getRequiredOption(c, FlagReason)
	if len(reason) == 0 {
		ErrorAndExit("wrong reason", fmt.Errorf("reason cannot be empty"))
	}
	eventID := c.Int64(FlagEventID)
	resetType := c.String(FlagResetType)
	extraForResetType, ok := resetTypesMap[resetType]
	if !ok && eventID <= 0 {
		ErrorAndExit("Must specify valid eventID or valid resetType", nil)
	}
	if ok && len(extraForResetType) > 0 {
		getRequiredOption(c, extraForResetType)
	}

	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.ServerFrontendClient(c)

	resetBaseRunID := rid
	decisionFinishID := eventID
	var err error
	if resetType != "" {
		resetBaseRunID, decisionFinishID, err = getResetEventIDByType(ctx, c, resetType, domain, wid, rid, frontendClient)
		if err != nil {
			ErrorAndExit("getResetEventIDByType failed", err)
		}
	}
	resp, err := frontendClient.ResetWorkflowExecution(ctx, &shared.ResetWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(resetBaseRunID),
		},
		Reason:                common.StringPtr(fmt.Sprintf("%v:%v", getCurrentUserFromEnv(), reason)),
		DecisionFinishEventId: common.Int64Ptr(decisionFinishID),
		RequestId:             common.StringPtr(uuid.New()),
	})
	if err != nil {
		ErrorAndExit("reset failed", err)
	}
	prettyPrintJSONObject(resp)
}

func processResets(c *cli.Context, domain string, wes chan shared.WorkflowExecution, done chan bool, wg *sync.WaitGroup, params batchResetParamsType) {
	for {
		select {
		case we := <-wes:
			fmt.Println("received: ", we.GetWorkflowId(), we.GetRunId())
			wid := we.GetWorkflowId()
			rid := we.GetRunId()
			var err error
			for i := 0; i < 3; i++ {
				err = doReset(c, domain, wid, rid, params)
				if err == nil {
					break
				}
				if _, ok := err.(*shared.BadRequestError); ok {
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
	domain := getRequiredGlobalOption(c, FlagDomain)
	resetType := getRequiredOption(c, FlagResetType)

	inFileName := c.String(FlagInputFile)
	query := c.String(FlagListQuery)
	excFileName := c.String(FlagExcludeFile)
	separator := c.String(FlagInputSeparator)
	parallel := c.Int(FlagParallism)

	extraForResetType, ok := resetTypesMap[resetType]
	if !ok {
		ErrorAndExit("Not supported reset type", nil)
	} else if len(extraForResetType) > 0 {
		getRequiredOption(c, extraForResetType)
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

	wes := make(chan shared.WorkflowExecution)
	done := make(chan bool)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go processResets(c, domain, wes, done, wg, batchResetParams)
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

			wes <- shared.WorkflowExecution{
				WorkflowId: common.StringPtr(wid),
				RunId:      common.StringPtr(rid),
			}
		}
	} else {
		wfClient := getWorkflowClient(c)
		pageSize := 1000
		var nextPageToken []byte
		var result []*s.WorkflowExecutionInfo
		for {
			result, nextPageToken = scanWorkflowExecutions(wfClient, pageSize, nextPageToken, query, c)
			for _, we := range result {
				wid := we.Execution.GetWorkflowId()
				rid := we.Execution.GetRunId()
				_, ok := excludes[wid]
				if ok {
					fmt.Println("skip by exclude file: ", wid, rid)
					continue
				}

				wes <- shared.WorkflowExecution{
					WorkflowId: common.StringPtr(wid),
					RunId:      common.StringPtr(rid),
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

// sort helper for search attributes
type byKey [][]string

func (s byKey) Len() int {
	return len(s)
}
func (s byKey) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byKey) Less(i, j int) bool {
	return s[i][0] < s[j][0]
}

func printErrorAndReturn(msg string, err error) error {
	fmt.Println(msg)
	return err
}

func doReset(c *cli.Context, domain, wid, rid string, params batchResetParamsType) error {
	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.ServerFrontendClient(c)
	resp, err := frontendClient.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
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

	if resp.WorkflowExecutionInfo.CloseStatus == nil || resp.WorkflowExecutionInfo.CloseTime == nil {
		if params.skipOpen {
			fmt.Println("skip because current run is open: ", wid, rid, currentRunID)
			//skip and not terminate current if open
			return nil
		}
	}

	if params.nonDeterministicOnly {
		isLDN, err := isLastEventDecisionTaskFailedWithNonDeterminism(ctx, domain, wid, rid, frontendClient)
		if err != nil {
			return printErrorAndReturn("check isLastEventDecisionTaskFailedWithNonDeterminism failed", err)
		}
		if !isLDN {
			fmt.Println("skip because last event is not DecisionTaskFailedWithNonDeterminism")
			return nil
		}
	}

	resetBaseRunID, decisionFinishID, err := getResetEventIDByType(ctx, c, params.resetType, domain, wid, rid, frontendClient)
	if err != nil {
		return printErrorAndReturn("getResetEventIDByType failed", err)
	}
	fmt.Println("DecisionFinishEventId for reset:", wid, rid, resetBaseRunID, decisionFinishID)

	if params.dryRun {
		fmt.Printf("dry run to reset wid: %v, rid:%v to baseRunID:%v, eventID:%v \n", wid, rid, resetBaseRunID, decisionFinishID)
	} else {
		resp2, err := frontendClient.ResetWorkflowExecution(ctx, &shared.ResetWorkflowExecutionRequest{
			Domain: common.StringPtr(domain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(wid),
				RunId:      common.StringPtr(resetBaseRunID),
			},
			DecisionFinishEventId: common.Int64Ptr(decisionFinishID),
			RequestId:             common.StringPtr(uuid.New()),
			Reason:                common.StringPtr(fmt.Sprintf("%v:%v", getCurrentUserFromEnv(), params.reason)),
		})

		if err != nil {
			return printErrorAndReturn("ResetWorkflowExecution failed", err)
		}
		fmt.Println("new runID for wid/rid is ,", wid, rid, resp2.GetRunId())
	}

	return nil
}

func isLastEventDecisionTaskFailedWithNonDeterminism(ctx context.Context, domain, wid, rid string, frontendClient workflowserviceclient.Interface) (bool, error) {
	req := &shared.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
		MaximumPageSize: common.Int32Ptr(1000),
		NextPageToken:   nil,
	}

	var firstEvent, decisionFailed *shared.HistoryEvent
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return false, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if firstEvent == nil {
				firstEvent = e
			}
			if e.GetEventType() == shared.EventTypeDecisionTaskFailed {
				decisionFailed = e
			} else if e.GetEventType() == shared.EventTypeDecisionTaskCompleted {
				decisionFailed = nil
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}

	if decisionFailed != nil {
		attr := decisionFailed.GetDecisionTaskFailedEventAttributes()
		if attr.GetCause() == shared.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure ||
			strings.Contains(string(attr.GetDetails()), "nondeterministic") {
			fmt.Printf("found non determnistic workflow wid:%v, rid:%v, orignalStartTime:%v \n", wid, rid, time.Unix(0, firstEvent.GetTimestamp()))
			return true, nil
		}
	}

	return false, nil
}

func getResetEventIDByType(ctx context.Context, c *cli.Context, resetType, domain, wid, rid string, frontendClient workflowserviceclient.Interface) (resetBaseRunID string, decisionFinishID int64, err error) {
	fmt.Println("resetType:", resetType)
	switch resetType {
	case "LastDecisionCompleted":
		resetBaseRunID, decisionFinishID, err = getLastDecisionCompletedID(ctx, domain, wid, rid, frontendClient)
		if err != nil {
			return
		}
	case "LastContinuedAsNew":
		resetBaseRunID, decisionFinishID, err = getLastContinueAsNewID(ctx, domain, wid, rid, frontendClient)
		if err != nil {
			return
		}
	case "FirstDecisionCompleted":
		resetBaseRunID, decisionFinishID, err = getFirstDecisionCompletedID(ctx, domain, wid, rid, frontendClient)
		if err != nil {
			return
		}
	case "BadBinary":
		binCheckSum := c.String(FlagResetBadBinaryChecksum)
		resetBaseRunID, decisionFinishID, err = getBadDecisionCompletedID(ctx, domain, wid, rid, binCheckSum, frontendClient)
		if err != nil {
			return
		}
	default:
		panic("not supported resetType")
	}
	return
}

func getLastDecisionCompletedID(ctx context.Context, domain, wid, rid string, frontendClient workflowserviceclient.Interface) (resetBaseRunID string, decisionFinishID int64, err error) {
	resetBaseRunID = rid
	req := &shared.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
		MaximumPageSize: common.Int32Ptr(1000),
		NextPageToken:   nil,
	}

	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if e.GetEventType() == shared.EventTypeDecisionTaskCompleted {
				decisionFinishID = e.GetEventId()
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}
	if decisionFinishID == 0 {
		return "", 0, printErrorAndReturn("Get DecisionFinishID failed", fmt.Errorf("no DecisionFinishID"))
	}
	return
}

func getBadDecisionCompletedID(ctx context.Context, domain, wid, rid, binChecksum string, frontendClient workflowserviceclient.Interface) (resetBaseRunID string, decisionFinishID int64, err error) {
	resetBaseRunID = rid
	resp, err := frontendClient.DescribeWorkflowExecution(ctx, &shared.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
	})
	if err != nil {
		return "", 0, printErrorAndReturn("DescribeWorkflowExecution failed", err)
	}

	_, p := history.FindAutoResetPoint(clock.NewRealTimeSource(), &shared.BadBinaries{
		Binaries: map[string]*shared.BadBinaryInfo{
			binChecksum: {},
		},
	}, resp.WorkflowExecutionInfo.AutoResetPoints)
	if p != nil {
		decisionFinishID = p.GetFirstDecisionCompletedId()
	}

	if decisionFinishID == 0 {
		return "", 0, printErrorAndReturn("Get DecisionFinishID failed", &shared.BadRequestError{"no DecisionFinishID"})
	}
	return
}

func getFirstDecisionCompletedID(ctx context.Context, domain, wid, rid string, frontendClient workflowserviceclient.Interface) (resetBaseRunID string, decisionFinishID int64, err error) {
	resetBaseRunID = rid
	req := &shared.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
		MaximumPageSize: common.Int32Ptr(1000),
		NextPageToken:   nil,
	}

	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if e.GetEventType() == shared.EventTypeDecisionTaskCompleted {
				decisionFinishID = e.GetEventId()
				return resetBaseRunID, decisionFinishID, nil
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}
	if decisionFinishID == 0 {
		return "", 0, printErrorAndReturn("Get DecisionFinishID failed", fmt.Errorf("no DecisionFinishID"))
	}
	return
}

func getLastContinueAsNewID(ctx context.Context, domain, wid, rid string, frontendClient workflowserviceclient.Interface) (resetBaseRunID string, decisionFinishID int64, err error) {
	// get first event
	req := &shared.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
		MaximumPageSize: common.Int32Ptr(1),
		NextPageToken:   nil,
	}
	resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
	if err != nil {
		return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
	}
	firstEvent := resp.History.Events[0]
	resetBaseRunID = firstEvent.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId()
	if resetBaseRunID == "" {
		return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", fmt.Errorf("cannot get resetBaseRunID"))
	}

	req = &shared.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domain),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(resetBaseRunID),
		},
		MaximumPageSize: common.Int32Ptr(1000),
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return "", 0, printErrorAndReturn("GetWorkflowExecutionHistory failed", err)
		}
		for _, e := range resp.GetHistory().GetEvents() {
			if e.GetEventType() == shared.EventTypeDecisionTaskCompleted {
				decisionFinishID = e.GetEventId()
			}
		}
		if len(resp.NextPageToken) != 0 {
			req.NextPageToken = resp.NextPageToken
		} else {
			break
		}
	}
	if decisionFinishID == 0 {
		return "", 0, printErrorAndReturn("Get DecisionFinishID failed", fmt.Errorf("no DecisionFinishID"))
	}
	return
}

// CompleteActivity completes an activity
func CompleteActivity(c *cli.Context) {
	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	activityID := getRequiredOption(c, FlagActivityID)
	if len(activityID) == 0 {
		ErrorAndExit("Invalid activityID", fmt.Errorf("activityID cannot be empty"))
	}
	result := getRequiredOption(c, FlagResult)
	identity := getRequiredOption(c, FlagIdentity)
	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.ServerFrontendClient(c)
	err := frontendClient.RespondActivityTaskCompletedByID(ctx, &shared.RespondActivityTaskCompletedByIDRequest{
		Domain:     common.StringPtr(domain),
		WorkflowID: common.StringPtr(wid),
		RunID:      common.StringPtr(rid),
		ActivityID: common.StringPtr(activityID),
		Result:     []byte(result),
		Identity:   common.StringPtr(identity),
	})
	if err != nil {
		ErrorAndExit("Completing activity failed", err)
	} else {
		fmt.Println("Complete activity successfully.")
	}
}

// FailActivity fails an activity
func FailActivity(c *cli.Context) {
	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := getRequiredOption(c, FlagRunID)
	activityID := getRequiredOption(c, FlagActivityID)
	if len(activityID) == 0 {
		ErrorAndExit("Invalid activityID", fmt.Errorf("activityID cannot be empty"))
	}
	reason := getRequiredOption(c, FlagReason)
	detail := getRequiredOption(c, FlagDetail)
	identity := getRequiredOption(c, FlagIdentity)
	ctx, cancel := newContext(c)
	defer cancel()

	frontendClient := cFactory.ServerFrontendClient(c)
	err := frontendClient.RespondActivityTaskFailedByID(ctx, &shared.RespondActivityTaskFailedByIDRequest{
		Domain:     common.StringPtr(domain),
		WorkflowID: common.StringPtr(wid),
		RunID:      common.StringPtr(rid),
		ActivityID: common.StringPtr(activityID),
		Reason:     common.StringPtr(reason),
		Details:    []byte(detail),
		Identity:   common.StringPtr(identity),
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
