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
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber/cadence/common/clock"

	"github.com/uber/cadence/service/history"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/urfave/cli"
	"github.com/valyala/fastjson"
	s "go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
)

const (
	localHostPort = "127.0.0.1:7933"

	maxOutputStringLength = 200 // max length for output string
	maxWorkflowTypeLength = 32  // max item length for output workflow type in table
	defaultMaxFieldLength = 500 // default max length for each attribute field
	maxWordLength         = 120 // if text length is larger than maxWordLength, it will be inserted spaces

	defaultTimeFormat                = "15:04:05"   // used for converting UnixNano to string like 16:16:36 (only time)
	defaultDateTimeFormat            = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
	defaultDomainRetentionDays       = 3
	defaultContextTimeoutInSeconds   = 5
	defaultContextTimeout            = defaultContextTimeoutInSeconds * time.Second
	defaultContextTimeoutForLongPoll = 2 * time.Minute

	defaultDecisionTimeoutInSeconds = 10
	defaultPageSizeForList          = 500
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

// SetFactory is used to set the ClientFactory global
func SetFactory(factory ClientFactory) {
	cFactory = factory
}

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

func mapKeysToArray(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ErrorAndExit print easy to understand error msg first then error detail in a new line
func ErrorAndExit(msg string, err error) {
	if err != nil {
		fmt.Printf("%s %s\n%s %+v\n", colorRed("Error:"), msg, colorMagenta("Error Details:"), err)
		if os.Getenv(showErrorStackEnv) != `` {
			fmt.Printf("Stack trace:\n")
			debug.PrintStack()
		} else {
			fmt.Printf("('export %s=1' to see stack traces)\n", showErrorStackEnv)
		}
	} else {
		fmt.Printf("%s %s\n", colorRed("Error:"), msg)
	}
	osExit(1)
}

// RegisterDomain register a domain
func RegisterDomain(c *cli.Context) {
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	description := c.String(FlagDescription)
	ownerEmail := c.String(FlagOwnerEmail)
	retentionDays := defaultDomainRetentionDays

	if c.IsSet(FlagRetentionDays) {
		retentionDays = c.Int(FlagRetentionDays)
	}
	securityToken := c.String(FlagSecurityToken)
	emitMetric := false
	var err error
	if c.IsSet(FlagEmitMetric) {
		emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagEmitMetric), err)
		}
	}
	isGlobalDomain := false
	if !c.IsSet(FlagIsGlobalDomain) {
		ErrorAndExit(fmt.Sprintf("Option %s must be provided.", FlagIsGlobalDomain), err)
	} else {
		isGlobalDomain, err = strconv.ParseBool(c.String(FlagIsGlobalDomain))
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagIsGlobalDomain), err)
		}
	}

	domainData := map[string]string{}
	if c.IsSet(FlagDomainData) {
		domainDataStr := getRequiredOption(c, FlagDomainData)
		domainData, err = parseDomainDataKVs(domainDataStr)
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagDomainData), err)
		}
	}
	if len(requiredDomainDataKeys) > 0 {
		err = checkRequiredDomainDataKVs(domainData)
		if err != nil {
			ErrorAndExit("Domain data missed required data.", err)
		}
	}

	var activeClusterName string
	if c.IsSet(FlagActiveClusterName) {
		activeClusterName = c.String(FlagActiveClusterName)
	}
	var clusters []*shared.ClusterReplicationConfiguration
	if c.IsSet(FlagClusters) {
		clusterStr := c.String(FlagClusters)
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterStr),
		})
		for _, clusterStr := range c.Args() {
			clusters = append(clusters, &shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterStr),
			})
		}
	}

	request := &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(domain),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(ownerEmail),
		Data:                                   domainData,
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
		SecurityToken:                          common.StringPtr(securityToken),
		ArchivalStatus:                         archivalStatus(c),
		ArchivalBucketName:                     common.StringPtr(c.String(FlagArchivalBucketName)),
		IsGlobalDomain:                         common.BoolPtr(isGlobalDomain),
	}

	ctx, cancel := newContext(c)
	defer cancel()
	err = frontendClient.RegisterDomain(ctx, request)
	if err != nil {
		if _, ok := err.(*s.DomainAlreadyExistsError); !ok {
			ErrorAndExit("Register Domain operation failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s already registered.", domain), err)
		}
	} else {
		fmt.Printf("Domain %s successfully registered.\n", domain)
	}
}

// UpdateDomain updates a domain
func UpdateDomain(c *cli.Context) {
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	var updateRequest *shared.UpdateDomainRequest
	ctx, cancel := newContext(c)
	defer cancel()

	if c.IsSet(FlagActiveClusterName) {
		activeCluster := c.String(FlagActiveClusterName)
		fmt.Printf("Will set active cluster name to: %s, other flag will be omitted.\n", activeCluster)
		replicationConfig := &shared.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(activeCluster),
		}
		updateRequest = &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			ReplicationConfiguration: replicationConfig,
		}
	} else {
		resp, err := frontendClient.DescribeDomain(ctx, &shared.DescribeDomainRequest{
			Name: common.StringPtr(domain),
		})
		if err != nil {
			if _, ok := err.(*shared.EntityNotExistsError); !ok {
				ErrorAndExit("Operation UpdateDomain failed.", err)
			} else {
				ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domain), err)
			}
			return
		}

		description := resp.DomainInfo.GetDescription()
		ownerEmail := resp.DomainInfo.GetOwnerEmail()
		retentionDays := resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays()
		emitMetric := resp.Configuration.GetEmitMetric()
		var clusters []*shared.ClusterReplicationConfiguration

		if c.IsSet(FlagDescription) {
			description = c.String(FlagDescription)
		}
		if c.IsSet(FlagOwnerEmail) {
			ownerEmail = c.String(FlagOwnerEmail)
		}
		domainData := map[string]string{}
		if c.IsSet(FlagDomainData) {
			domainDataStr := c.String(FlagDomainData)
			domainData, err = parseDomainDataKVs(domainDataStr)
			if err != nil {
				ErrorAndExit("Domain data format is invalid.", err)
			}
		}
		if c.IsSet(FlagRetentionDays) {
			retentionDays = int32(c.Int(FlagRetentionDays))
		}
		if c.IsSet(FlagEmitMetric) {
			emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
			if err != nil {
				ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagEmitMetric), err)
			}
		}
		if c.IsSet(FlagClusters) {
			clusterStr := c.String(FlagClusters)
			clusters = append(clusters, &shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterStr),
			})
			for _, clusterStr := range c.Args() {
				clusters = append(clusters, &shared.ClusterReplicationConfiguration{
					ClusterName: common.StringPtr(clusterStr),
				})
			}
		}

		var binBinaries *shared.BadBinaries
		if c.IsSet(FlagAddBadBinary) {
			if !c.IsSet(FlagReason) {
				ErrorAndExit("Must provide a reason.", nil)
			}
			binChecksum := c.String(FlagAddBadBinary)
			reason := c.String(FlagReason)
			operator := getCurrentUserFromEnv()
			binBinaries = &shared.BadBinaries{
				Binaries: map[string]*shared.BadBinaryInfo{
					binChecksum: {
						Reason:   common.StringPtr(reason),
						Operator: common.StringPtr(operator),
					},
				},
			}
		}

		var badBinaryToDelete *string
		if c.IsSet(FlagRemoveBadBinary) {
			badBinaryToDelete = common.StringPtr(c.String(FlagRemoveBadBinary))
		}

		updateInfo := &shared.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(ownerEmail),
			Data:        domainData,
		}
		updateConfig := &shared.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
			EmitMetric:                             common.BoolPtr(emitMetric),
			ArchivalStatus:                         archivalStatus(c),
			ArchivalBucketName:                     common.StringPtr(c.String(FlagArchivalBucketName)),
			BadBinaries:                            binBinaries,
		}
		replicationConfig := &shared.DomainReplicationConfiguration{
			Clusters: clusters,
		}
		updateRequest = &shared.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			UpdatedInfo:              updateInfo,
			Configuration:            updateConfig,
			ReplicationConfiguration: replicationConfig,
			DeleteBadBinary:          badBinaryToDelete,
		}
	}

	securityToken := c.String(FlagSecurityToken)
	updateRequest.SecurityToken = common.StringPtr(securityToken)
	_, err := frontendClient.UpdateDomain(ctx, updateRequest)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			ErrorAndExit("Operation UpdateDomain failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domain), err)
		}
	} else {
		fmt.Printf("Domain %s successfully updated.\n", domain)
	}
}

func getCurrentUserFromEnv() string {
	for _, n := range envKeysForUserName {
		if len(os.Getenv(n)) > 0 {
			return os.Getenv(n)
		}
	}
	return "unkown"
}

// DescribeDomain updates a domain
func DescribeDomain(c *cli.Context) {
	domainName := c.GlobalString(FlagDomain)
	domainID := c.String(FlagDomainID)

	if domainID == "" && domainName == "" {
		ErrorAndExit("At least domainID or domainName must be provided.", nil)
	}
	ctx, cancel := newContext(c)
	defer cancel()
	frontendClient := cFactory.ServerFrontendClient(c)
	resp, err := frontendClient.DescribeDomain(ctx, &shared.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
		UUID: common.StringPtr(domainID),
	})
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			ErrorAndExit("Operation DescribeDomain failed.", err)
		}
		ErrorAndExit(fmt.Sprintf("Domain %s does not exist.", domainName), err)
	}

	var formatStr = "Name: %v\nUUID: %v\nDescription: %v\nOwnerEmail: %v\nDomainData: %v\nStatus: %v\nRetentionInDays: %v\n" +
		"EmitMetrics: %v\nActiveClusterName: %v\nClusters: %v\nArchivalStatus: %v\n"
	descValues := []interface{}{
		resp.DomainInfo.GetName(),
		resp.DomainInfo.GetUUID(),
		resp.DomainInfo.GetDescription(),
		resp.DomainInfo.GetOwnerEmail(),
		resp.DomainInfo.Data,
		resp.DomainInfo.GetStatus(),
		resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays(),
		resp.Configuration.GetEmitMetric(),
		resp.ReplicationConfiguration.GetActiveClusterName(),
		clustersToString(resp.ReplicationConfiguration.Clusters),
		resp.Configuration.GetArchivalStatus().String(),
	}
	if resp.Configuration.GetArchivalBucketName() != "" {
		formatStr = formatStr + "BucketName: %v\n"
		descValues = append(descValues, resp.Configuration.GetArchivalBucketName())
	}
	fmt.Printf(formatStr, descValues...)
	if resp.Configuration.BadBinaries != nil {
		fmt.Println("Bad binaries to reset:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetBorder(true)
		table.SetColumnSeparator("|")
		header := []string{"Binary Checksum", "Operator", "Start Time", "Reason"}
		headerColor := []tablewriter.Colors{tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue}
		table.SetHeader(header)
		table.SetHeaderColor(headerColor...)
		for cs, bin := range resp.Configuration.BadBinaries.Binaries {
			row := []string{cs}
			row = append(row, bin.GetOperator())
			row = append(row, time.Unix(0, bin.GetCreatedTimeNano()).String())
			row = append(row, bin.GetReason())
			table.Append(row)
		}
		table.Render()
	}
}

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
	queryResponse, err := serviceClient.QueryWorkflow(tcCtx, queryRequest)
	if err != nil {
		ErrorAndExit("Query workflow failed.", err)
		return
	}

	// assume it is json encoded
	fmt.Printf("Query result as JSON:\n%v\n", string(queryResponse.QueryResult))
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
			printListResults(results, printJSON)
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

			fmt.Printf("Press %s to show next page, press %s to quit: ",
				color.GreenString("Enter"), color.RedString("any other key then Enter"))
			var input string
			fmt.Scanln(&input)
			if strings.Trim(input, " ") == "" {
				continue
			} else {
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
			printListResults(results, printJSON)
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
	printRawTime := c.Bool(FlagPrintRawTime) // default show datetime instead of raw time
	resetPontsOnly := c.Bool(FlagResetPointsOnly)

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
	var o interface{}
	if printRawTime {
		o = resp
	} else {
		o = convertDescribeWorkflowExecutionResponse(resp)
	}

	if !resetPontsOnly {
		prettyPrintJSONObject(o)
	} else {
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

}

func prettyPrintJSONObject(o interface{}) {
	b, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		fmt.Printf("Error when try to print pretty: %v\n", err)
		fmt.Println(o)
	}
	os.Stdout.Write(b)
	fmt.Println()
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
	Execution       *shared.WorkflowExecution
	Type            *shared.WorkflowType
	StartTime       *string // change from *int64
	CloseTime       *string // change from *int64
	CloseStatus     *shared.WorkflowExecutionCloseStatus
	HistoryLength   *int64
	ParentDomainID  *string
	ParentExecution *shared.WorkflowExecution
	AutoResetPoints *shared.ResetPoints
}

// pendingActivityInfo has same fields as shared.PendingActivityInfo, but different field type for better display
type pendingActivityInfo struct {
	ActivityID             *string
	ActivityType           *shared.ActivityType
	State                  *shared.PendingActivityState
	ScheduledTimestamp     *string `json:",omitempty"` // change from *int64
	LastStartedTimestamp   *string `json:",omitempty"` // change from *int64
	HeartbeatDetails       *string `json:",omitempty"` // change from byte[]
	LastHeartbeatTimestamp *string `json:",omitempty"` // change from *int64
	Attempt                *int32  `json:",omitempty"`
	MaximumAttempts        *int32  `json:",omitempty"`
	ExpirationTimestamp    *string `json:",omitempty"` // change from *int64
	LastFailureReason      *string `json:",omitempty"`
	LastWorkerIdentity     *string `json:",omitempty"`
}

func convertDescribeWorkflowExecutionResponse(resp *shared.DescribeWorkflowExecutionResponse) *describeWorkflowExecutionResponse {
	info := resp.WorkflowExecutionInfo
	executionInfo := workflowExecutionInfo{
		Execution:       info.Execution,
		Type:            info.Type,
		StartTime:       common.StringPtr(convertTime(info.GetStartTime(), false)),
		CloseTime:       common.StringPtr(convertTime(info.GetCloseTime(), false)),
		CloseStatus:     info.CloseStatus,
		HistoryLength:   info.HistoryLength,
		ParentDomainID:  info.ParentDomainId,
		ParentExecution: info.ParentExecution,
		AutoResetPoints: info.AutoResetPoints,
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
		pendingActs = append(pendingActs, tmpAct)
	}

	return &describeWorkflowExecutionResponse{
		ExecutionConfiguration: resp.ExecutionConfiguration,
		WorkflowExecutionInfo:  executionInfo,
		PendingActivities:      pendingActs,
		PendingChildren:        resp.PendingChildren,
	}
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
	if queryOpen {
		result, nextPageToken = listOpenWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, nextPageToken, c)
	} else {
		result, nextPageToken = listClosedWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, workflowStatus, nextPageToken, c)
	}

	return result, nextPageToken
}

// default will print decoded raw
func printListResults(executions []*s.WorkflowExecutionInfo, inJSON bool) {
	for i, execution := range executions {
		if inJSON {
			j, _ := json.Marshal(execution)
			if i < len(executions)-1 {
				fmt.Println(string(j) + ",")
			} else {
				fmt.Println(string(j))
			}
		} else {
			if i < len(executions)-1 {
				fmt.Println(anyToString(execution, true, 0) + ",")
			} else {
				fmt.Println(anyToString(execution, true, 0))
			}
		}
	}
}

// DescribeTaskList show pollers info of a given tasklist
func DescribeTaskList(c *cli.Context) {
	wfClient := getWorkflowClient(c)
	taskList := getRequiredOption(c, FlagTaskList)
	taskListType := strToTaskListType(c.String(FlagTaskListType)) // default type is decision

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := wfClient.DescribeTaskList(ctx, taskList, taskListType)
	if err != nil {
		ErrorAndExit("Operation DescribeTaskList failed.", err)
	}

	pollers := response.Pollers
	if len(pollers) == 0 {
		ErrorAndExit(colorMagenta("No poller for tasklist: "+taskList), nil)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	if taskListType == s.TaskListTypeActivity {
		table.SetHeader([]string{"Activity Poller Identity", "Last Access Time"})
	} else {
		table.SetHeader([]string{"Decision Poller Identity", "Last Access Time"})
	}
	table.SetHeaderLine(false)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	for _, poller := range pollers {
		table.Append([]string{poller.GetIdentity(), convertTime(poller.GetLastAccessTime(), false)})
	}
	table.Render()
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

func processResets(c *cli.Context, domain string, wes chan shared.WorkflowExecution, done chan bool, wg *sync.WaitGroup, reason, resetType string, skipOpen bool) {
	for {
		select {
		case we := <-wes:
			fmt.Println("received: ", we.GetWorkflowId(), we.GetRunId())
			wid := we.GetWorkflowId()
			rid := we.GetRunId()
			var err error
			for i := 0; i < 3; i++ {
				err = doReset(c, domain, wid, rid, reason, resetType, skipOpen)
				if err == nil {
					break
				}
				fmt.Println("failed and retry...: ", wid, rid, err)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(2000)))
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
			if err != nil {
				fmt.Println("[ERROR] failed processing: ", wid, rid)
			}
		case <-done:
			wg.Done()
			return
		}
	}
}

// ResetInBatch resets workflow in batch
func ResetInBatch(c *cli.Context) {
	domain := getRequiredGlobalOption(c, FlagDomain)
	inFileName := getRequiredOption(c, FlagInputFile)
	excFileName := getRequiredOption(c, FlagExcludeFile)
	separator := getRequiredOption(c, FlagInputSeparator)
	reason := getRequiredOption(c, FlagReason)
	resetType := getRequiredOption(c, FlagResetType)
	extraForResetType, ok := resetTypesMap[resetType]
	if !ok {
		ErrorAndExit("Not supported reset type", nil)
	} else {
		getRequiredOption(c, extraForResetType)
	}

	if !c.IsSet(FlagSkipCurrent) {
		ErrorAndExit("need to specify whether skip on current is open", nil)
	}
	skipOpen := c.Bool(FlagSkipCurrent)

	parallel := c.Int(FlagParallism)
	wg := &sync.WaitGroup{}

	wes := make(chan shared.WorkflowExecution)
	done := make(chan bool)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go processResets(c, domain, wes, done, wg, reason, resetType, skipOpen)
	}

	// read exclude
	excludes := map[string]string{}
	if len(excFileName) > 0 {
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
			fmt.Println("already processed, skip: ", wid, rid)
			continue
		}

		wes <- shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
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

// GetSearchAttributes get valid search attributes
func GetSearchAttributes(c *cli.Context) {
	wfClient := getWorkflowClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := wfClient.GetSearchAttributes(ctx)
	if err != nil {
		ErrorAndExit("Failed to get search attributes.", err)
	}

	table := tablewriter.NewWriter(os.Stdout)
	header := []string{"Key", "Value type"}
	table.SetHeader(header)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	rows := [][]string{}
	for k, v := range resp.Keys {
		rows = append(rows, []string{k, v.String()})
	}
	sort.Sort(byKey(rows))
	table.AppendBulk(rows)
	table.Render()
}

func printErrorAndReturn(msg string, err error) error {
	fmt.Println(msg)
	return err
}

func doReset(c *cli.Context, domain, wid, rid string, reason, resetType string, skipOpen bool) error {
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
	if currentRunID == rid {
		fmt.Println("current run is the reset run: ", wid, rid)
		//return nil
	}
	if rid == "" {
		rid = currentRunID
	}

	if resp.WorkflowExecutionInfo.CloseStatus == nil || resp.WorkflowExecutionInfo.CloseTime == nil {
		if skipOpen {
			fmt.Println("skip because current run is open: ", wid, rid, currentRunID)
			//skip and not terminate current if open
			return nil
		}
	}

	resetBaseRunID, decisionFinishID, err := getResetEventIDByType(ctx, c, resetType, domain, wid, rid, frontendClient)
	if err != nil {
		return printErrorAndReturn("getResetEventIDByType failed", err)
	}
	fmt.Println("DecisionFinishEventId for reset:", wid, rid, resetBaseRunID, decisionFinishID)

	resp2, err := frontendClient.ResetWorkflowExecution(ctx, &shared.ResetWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(resetBaseRunID),
		},
		DecisionFinishEventId: common.Int64Ptr(decisionFinishID),
		RequestId:             common.StringPtr(uuid.New()),
		Reason:                common.StringPtr(fmt.Sprintf("%v:%v", getCurrentUserFromEnv(), reason)),
	})

	if err != nil {
		return printErrorAndReturn("ResetWorkflowExecution failed", err)
	}
	fmt.Println("new runID for wid/rid is ,", wid, rid, resp2.GetRunId())
	return nil
}

func getResetEventIDByType(ctx context.Context, c *cli.Context, resetType, domain, wid, rid string, frontendClient workflowserviceclient.Interface) (resetBaseRunID string, decisionFinishID int64, err error) {
	fmt.Println("switch", resetType)
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
		return "", 0, printErrorAndReturn("Get DecisionFinishID failed", fmt.Errorf("no DecisionFinishID"))
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

func getWorkflowClient(c *cli.Context) client.Client {
	domain := getRequiredGlobalOption(c, FlagDomain)
	return client.NewClient(cFactory.ClientFrontendClient(c), domain, &client.Options{})
}

func getRequiredOption(c *cli.Context, optionName string) string {
	value := c.String(optionName)
	if len(value) == 0 {
		ErrorAndExit(fmt.Sprintf("Option %s is required", optionName), nil)
	}
	return value
}

func getRequiredGlobalOption(c *cli.Context, optionName string) string {
	value := c.GlobalString(optionName)
	if len(value) == 0 {
		ErrorAndExit(fmt.Sprintf("Global option %s is required", optionName), nil)
	}
	return value
}

func timestampPtrToStringPtr(unixNanoPtr *int64, onlyTime bool) *string {
	if unixNanoPtr == nil {
		return nil
	}
	return common.StringPtr(convertTime(*unixNanoPtr, onlyTime))
}

func convertTime(unixNano int64, onlyTime bool) string {
	t := time.Unix(0, unixNano)
	var result string
	if onlyTime {
		result = t.Format(defaultTimeFormat)
	} else {
		result = t.Format(defaultDateTimeFormat)
	}
	return result
}

func parseTime(timeStr string, defaultValue int64) int64 {
	if len(timeStr) == 0 {
		return defaultValue
	}

	// try to parse
	parsedTime, err := time.Parse(defaultDateTimeFormat, timeStr)
	if err == nil {
		return parsedTime.UnixNano()
	}

	// treat as raw time
	resultValue, err := strconv.ParseInt(timeStr, 10, 64)
	if err != nil {
		ErrorAndExit(fmt.Sprintf("Cannot parse time '%s', use UTC format '2006-01-02T15:04:05Z' or raw UnixNano directly.", timeStr), err)
	}

	return resultValue
}

func strToTaskListType(str string) s.TaskListType {
	if strings.ToLower(str) == "activity" {
		return s.TaskListTypeActivity
	}
	return s.TaskListTypeDecision
}

func getPtrOrNilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return common.StringPtr(value)
}

func getCliIdentity() string {
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "UnKnown"
	}
	return fmt.Sprintf("cadence-cli@%s", hostName)
}

func newContext(c *cli.Context) (context.Context, context.CancelFunc) {
	contextTimeout := defaultContextTimeout
	if c.GlobalInt(FlagContextTimeout) > 0 {
		contextTimeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}
	return context.WithTimeout(context.Background(), contextTimeout)
}

func newContextForLongPoll(c *cli.Context) (context.Context, context.CancelFunc) {
	contextTimeout := defaultContextTimeoutForLongPoll
	if c.GlobalIsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.GlobalInt(FlagContextTimeout)) * time.Second
	}
	return context.WithTimeout(context.Background(), contextTimeout)
}

// process and validate input provided through cmd or file
func processJSONInput(c *cli.Context) string {
	return processJSONInputHelper(c, jsonTypeInput)
}

// process and validate json
func processJSONInputHelper(c *cli.Context, jType jsonType) string {
	var flagNameOfRawInput string
	var flagNameOfInputFileName string

	switch jType {
	case jsonTypeInput:
		flagNameOfRawInput = FlagInput
		flagNameOfInputFileName = FlagInputFile
	case jsonTypeMemo:
		flagNameOfRawInput = FlagMemo
		flagNameOfInputFileName = FlagMemoFile
	default:
		return ""
	}

	var input string
	if c.IsSet(flagNameOfRawInput) {
		input = c.String(flagNameOfRawInput)
	} else if c.IsSet(flagNameOfInputFileName) {
		inputFile := c.String(flagNameOfInputFileName)
		data, err := ioutil.ReadFile(inputFile)
		if err != nil {
			ErrorAndExit("Error reading input file", err)
		}
		input = string(data)
	}
	if input != "" {
		if err := validateJSONs(input); err != nil {
			ErrorAndExit("Input is not valid JSON, or JSONs concatenated with spaces/newlines.", err)
		}
	}
	return input
}

// validate whether str is a valid json or multi valid json concatenated with spaces/newlines
func validateJSONs(str string) error {
	input := []byte(str)
	dec := json.NewDecoder(bytes.NewReader(input))
	for {
		_, err := dec.Token()
		if err == io.EOF {
			return nil // End of input, valid JSON
		}
		if err != nil {
			return err // Invalid input
		}
	}
}

// use parseBool to ensure all BOOL search attributes only be "true" or "false"
func parseBool(str string) (bool, error) {
	switch str {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("not parseable bool value: %s", str)
}

func trimSpace(strs []string) []string {
	result := make([]string, len(strs))
	for i, v := range strs {
		result[i] = strings.TrimSpace(v)
	}
	return result
}

func convertStringToRealType(v string) interface{} {
	var genVal interface{}
	var err error

	if genVal, err = strconv.ParseInt(v, 10, 64); err == nil {

	} else if genVal, err = parseBool(v); err == nil {

	} else if genVal, err = strconv.ParseFloat(v, 64); err == nil {

	} else if genVal, err = time.Parse(defaultDateTimeFormat, v); err == nil {

	} else {
		genVal = v
	}

	return genVal
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

func truncate(str string) string {
	if len(str) > maxOutputStringLength {
		return str[:maxOutputStringLength]
	}
	return str
}

// this only works for ANSI terminal, which means remove existing lines won't work if users redirect to file
// ref: https://en.wikipedia.org/wiki/ANSI_escape_code
func removePrevious2LinesFromTerminal() {
	fmt.Printf("\033[1A")
	fmt.Printf("\033[2K")
	fmt.Printf("\033[1A")
	fmt.Printf("\033[2K")
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

func clustersToString(clusters []*shared.ClusterReplicationConfiguration) string {
	var res string
	for i, cluster := range clusters {
		if i == 0 {
			res = res + cluster.GetClusterName()
		} else {
			res = res + ", " + cluster.GetClusterName()
		}
	}
	return res
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

func archivalStatus(c *cli.Context) *shared.ArchivalStatus {
	if c.IsSet(FlagArchivalStatus) {
		switch c.String(FlagArchivalStatus) {
		case "disabled":
			return common.ArchivalStatusPtr(shared.ArchivalStatusDisabled)
		case "enabled":
			return common.ArchivalStatusPtr(shared.ArchivalStatusEnabled)
		default:
			ErrorAndExit(fmt.Sprintf("Option %s format is invalid.", FlagArchivalStatus), errors.New("invalid status, valid values are \"disabled\" and \"enabled\""))
		}
	}
	return nil
}
