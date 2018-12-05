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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/uber/cadence/common"
	"github.com/urfave/cli"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	s "go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
)

/**
Flags used to specify cli command line arguments
*/
const (
	FlagPort                       = "port"
	FlagUsername                   = "username"
	FlagPassword                   = "password"
	FlagKeyspace                   = "keyspace"
	FlagAddress                    = "address"
	FlagAddressWithAlias           = FlagAddress + ", ad"
	FlagHistoryAddress             = "history_address"
	FlagHistoryAddressWithAlias    = FlagHistoryAddress + ", had"
	FlagDomainID                   = "domain_id"
	FlagDomain                     = "domain"
	FlagDomainWithAlias            = FlagDomain + ", do"
	FlagShardID                    = "shard_id"
	FlagShardIDWithAlias           = FlagShardID + ", sid"
	FlagWorkflowID                 = "workflow_id"
	FlagWorkflowIDWithAlias        = FlagWorkflowID + ", wid, w"
	FlagRunID                      = "run_id"
	FlagTreeID                     = "tree_id"
	FlagBranchID                   = "branch_id"
	FlagNumberOfShards             = "number_of_shards"
	FlagRunIDWithAlias             = FlagRunID + ", rid, r"
	FlagTargetCluster              = "target_cluster"
	FlagMinEventID                 = "min_event_id"
	FlagMaxEventID                 = "max_event_id"
	FlagTaskList                   = "tasklist"
	FlagTaskListWithAlias          = FlagTaskList + ", tl"
	FlagTaskListType               = "tasklisttype"
	FlagTaskListTypeWithAlias      = FlagTaskListType + ", tlt"
	FlagWorkflowType               = "workflow_type"
	FlagWorkflowTypeWithAlias      = FlagWorkflowType + ", wt"
	FlagWorkflowStatus             = "status"
	FlagWorkflowStatusWithAlias    = FlagWorkflowStatus + ", s"
	FlagExecutionTimeout           = "execution_timeout"
	FlagExecutionTimeoutWithAlias  = FlagExecutionTimeout + ", et"
	FlagDecisionTimeout            = "decision_timeout"
	FlagDecisionTimeoutWithAlias   = FlagDecisionTimeout + ", dt"
	FlagContextTimeout             = "context_timeout"
	FlagContextTimeoutWithAlias    = FlagContextTimeout + ", ct"
	FlagInput                      = "input"
	FlagInputWithAlias             = FlagInput + ", i"
	FlagInputFile                  = "input_file"
	FlagInputFileWithAlias         = FlagInputFile + ", if"
	FlagInputTopic                 = "input_topic"
	FlagInputTopicWithAlias        = FlagInputTopic + ", it"
	FlagHostFile                   = "host_file"
	FlagCluster                    = "cluster"
	FlagInputCluster               = "input_cluster"
	FlagStartOffset                = "start_offset"
	FlagTopic                      = "topic"
	FlagGroup                      = "group"
	FlagReason                     = "reason"
	FlagReasonWithAlias            = FlagReason + ", re"
	FlagOpen                       = "open"
	FlagOpenWithAlias              = FlagOpen + ", op"
	FlagMore                       = "more"
	FlagMoreWithAlias              = FlagMore + ", m"
	FlagPageSize                   = "pagesize"
	FlagPageSizeWithAlias          = FlagPageSize + ", ps"
	FlagEarliestTime               = "earliest_time"
	FlagEarliestTimeWithAlias      = FlagEarliestTime + ", et"
	FlagLatestTime                 = "latest_time"
	FlagLatestTimeWithAlias        = FlagLatestTime + ", lt"
	FlagPrintEventVersion          = "print_event_version"
	FlagPrintEventVersionWithAlias = FlagPrintEventVersion + ", pev"
	FlagPrintFullyDetail           = "print_full"
	FlagPrintFullyDetailWithAlias  = FlagPrintFullyDetail + ", pf"
	FlagPrintRawTime               = "print_raw_time"
	FlagPrintRawTimeWithAlias      = FlagPrintRawTime + ", prt"
	FlagPrintDateTime              = "print_datetime"
	FlagPrintDateTimeWithAlias     = FlagPrintDateTime + ", pdt"
	FlagDescription                = "description"
	FlagDescriptionWithAlias       = FlagDescription + ", desc"
	FlagOwnerEmail                 = "owner_email"
	FlagOwnerEmailWithAlias        = FlagOwnerEmail + ", oe"
	FlagRetentionDays              = "retention"
	FlagRetentionDaysWithAlias     = FlagRetentionDays + ", rd"
	FlagEmitMetric                 = "emit_metric"
	FlagEmitMetricWithAlias        = FlagEmitMetric + ", em"
	FlagName                       = "name"
	FlagNameWithAlias              = FlagName + ", n"
	FlagOutputFilename             = "output_filename"
	FlagOutputFilenameWithAlias    = FlagOutputFilename + ", of"
	FlagQueryType                  = "query_type"
	FlagQueryTypeWithAlias         = FlagQueryType + ", qt"
	FlagShowDetail                 = "show_detail"
	FlagShowDetailWithAlias        = FlagShowDetail + ", sd"
	FlagActiveClusterName          = "active_cluster"
	FlagActiveClusterNameWithAlias = FlagActiveClusterName + ", ac"
	FlagClusters                   = "clusters"
	FlagClustersWithAlias          = FlagClusters + ", cl"
	FlagDomainData                 = "domain_data"
	FlagDomainDataWithAlias        = FlagDomainData + ", dmd"
	FlagEventID                    = "event_id"
	FlagEventIDWithAlias           = FlagEventID + ", eid"
	FlagMaxFieldLength             = "max_field_length"
	FlagMaxFieldLengthWithAlias    = FlagMaxFieldLength + ", maxl"
	FlagSecurityToken              = "security_token"
	FlagSecurityTokenWithAlias     = FlagSecurityToken + ", st"
	FlagSkipErrorMode              = "skip_errors"
	FlagSkipErrorModeWithAlias     = FlagSkipErrorMode + ", serr"
	FlagHeadersMode                = "headers"
	FlagHeadersModeWithAlias       = FlagHeadersMode + ", he"
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
	defaultContextTimeoutForLongPoll = 2 * time.Minute
	defaultDecisionTimeoutInSeconds  = 10
	defaultPageSizeForList           = 500

	workflowStatusNotSet = -1
	showErrorStackEnv    = `CADENCE_CLI_SHOW_STACKS`
)

// For color output to terminal
var (
	colorRed     = color.New(color.FgRed).SprintFunc()
	colorMagenta = color.New(color.FgMagenta).SprintFunc()
	colorGreen   = color.New(color.FgGreen).SprintFunc()

	tableHeaderBlue = tablewriter.Colors{tablewriter.FgHiBlueColor}
)

var optionErr = "there is something wrong with your command options"

var workflowClosedStatusMap = map[string]s.WorkflowExecutionCloseStatus{
	"completed":      s.WorkflowExecutionCloseStatusCompleted,
	"failed":         s.WorkflowExecutionCloseStatusFailed,
	"canceled":       s.WorkflowExecutionCloseStatusCanceled,
	"terminated":     s.WorkflowExecutionCloseStatusTerminated,
	"continuedasnew": s.WorkflowExecutionCloseStatusContinuedAsNew,
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

// cBuilder is used to create cadence clients
// To provide customized builder, call SetBuilder() before call NewCliApp()
var cBuilder WorkflowClientBuilderInterface

// osExit is used when CLI hits an error and exit
// The purpose of this is to test CLI exit scenario
var osExit = os.Exit

// SetBuilder can be used to inject customized builder of cadence clients
func SetBuilder(builder WorkflowClientBuilderInterface) {
	cBuilder = builder
}

// ErrorAndExit print easy to understand error msg first then error detail in a new line
func ErrorAndExit(msg string, err error) {
	if err != nil {
		if os.Getenv(showErrorStackEnv) != `` {
			fmt.Printf("%s %s\n%s %+v\n", colorRed("Error:"), msg, colorMagenta("Error Details:"), err)
		} else {
			fmt.Printf("%s %s\n('export %s=1' to see stack traces)\n", colorRed("Error:"), msg, showErrorStackEnv)
		}
	} else {
		fmt.Printf("%s %s\n", colorRed("Error:"), msg)
	}
	osExit(1)
}

// RegisterDomain register a domain
func RegisterDomain(c *cli.Context) {
	domainClient := getDomainClient(c)
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
			ErrorAndExit(FlagEmitMetric+" format is invalid.", err)
		}
	}

	domainData := map[string]string{}
	if c.IsSet(FlagDomainData) {
		domainDataStr := getRequiredOption(c, FlagDomainData)
		domainData, err = parseDomainDataKVs(domainDataStr)
		if err != nil {
			ErrorAndExit(FlagDomainData+" format is invalid.", err)
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
	var clusters []*s.ClusterReplicationConfiguration
	if c.IsSet(FlagClusters) {
		clusterStr := c.String(FlagClusters)
		clusters = append(clusters, &s.ClusterReplicationConfiguration{
			ClusterName: common.StringPtr(clusterStr),
		})
		for _, clusterStr := range c.Args() {
			clusters = append(clusters, &s.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterStr),
			})
		}
	}

	request := &s.RegisterDomainRequest{
		Name:                                   common.StringPtr(domain),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(ownerEmail),
		Data:                                   domainData,
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
		EmitMetric:                             common.BoolPtr(emitMetric),
		Clusters:                               clusters,
		ActiveClusterName:                      common.StringPtr(activeClusterName),
		SecurityToken:                          common.StringPtr(securityToken),
	}

	ctx, cancel := newContext()
	defer cancel()
	err = domainClient.Register(ctx, request)
	if err != nil {
		if _, ok := err.(*s.DomainAlreadyExistsError); !ok {
			ErrorAndExit("Register Domain operation failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s already registered.\n", domain), err)
		}
	} else {
		fmt.Printf("Domain %s successfully registered.\n", domain)
	}
}

// UpdateDomain updates a domain
func UpdateDomain(c *cli.Context) {
	domainClient := getDomainClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	var updateRequest *s.UpdateDomainRequest
	ctx, cancel := newContext()
	defer cancel()

	if c.IsSet(FlagActiveClusterName) {
		activeCluster := c.String(FlagActiveClusterName)
		fmt.Printf("Will set active cluster name to: %s, other flag will be omitted.\n", activeCluster)
		replicationConfig := &s.DomainReplicationConfiguration{
			ActiveClusterName: common.StringPtr(activeCluster),
		}
		updateRequest = &s.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			ReplicationConfiguration: replicationConfig,
		}
	} else {
		resp, err := domainClient.Describe(ctx, domain)
		if err != nil {
			if _, ok := err.(*s.EntityNotExistsError); !ok {
				ErrorAndExit("Operation UpdateDomain failed.", err)
			} else {
				ErrorAndExit(fmt.Sprintf("Domain %s does not exist.\n", domain), err)
			}
			return
		}

		description := resp.DomainInfo.GetDescription()
		ownerEmail := resp.DomainInfo.GetOwnerEmail()
		retentionDays := resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays()
		emitMetric := resp.Configuration.GetEmitMetric()
		var clusters []*s.ClusterReplicationConfiguration

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
				ErrorAndExit(FlagEmitMetric+"format is invalid.", err)
			}
		}
		if c.IsSet(FlagClusters) {
			clusterStr := c.String(FlagClusters)
			clusters = append(clusters, &s.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(clusterStr),
			})
			for _, clusterStr := range c.Args() {
				clusters = append(clusters, &s.ClusterReplicationConfiguration{
					ClusterName: common.StringPtr(clusterStr),
				})
			}
		}

		updateInfo := &s.UpdateDomainInfo{
			Description: common.StringPtr(description),
			OwnerEmail:  common.StringPtr(ownerEmail),
			Data:        domainData,
		}
		updateConfig := &s.DomainConfiguration{
			WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
			EmitMetric:                             common.BoolPtr(emitMetric),
		}
		replicationConfig := &s.DomainReplicationConfiguration{
			Clusters: clusters,
		}
		updateRequest = &s.UpdateDomainRequest{
			Name:                     common.StringPtr(domain),
			UpdatedInfo:              updateInfo,
			Configuration:            updateConfig,
			ReplicationConfiguration: replicationConfig,
		}
	}

	securityToken := c.String(FlagSecurityToken)
	updateRequest.SecurityToken = common.StringPtr(securityToken)
	err := domainClient.Update(ctx, updateRequest)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			ErrorAndExit("Operation UpdateDomain failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.\n", domain), err)
		}
	} else {
		fmt.Printf("Domain %s successfully updated.\n", domain)
	}
}

// DescribeDomain updates a domain
func DescribeDomain(c *cli.Context) {
	domainClient := getDomainClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	ctx, cancel := newContext()
	defer cancel()
	resp, err := domainClient.Describe(ctx, domain)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			ErrorAndExit("Operation DescribeDomain failed.", err)
		} else {
			ErrorAndExit(fmt.Sprintf("Domain %s does not exist.\n", domain), err)
		}
	} else {
		fmt.Printf("Name: %v\nDescription: %v\nOwnerEmail: %v\nDomainData: %v\nStatus: %v\nRetentionInDays: %v\n"+
			"EmitMetrics: %v\nActiveClusterName: %v\nClusters: %v\n",
			resp.DomainInfo.GetName(),
			resp.DomainInfo.GetDescription(),
			resp.DomainInfo.GetOwnerEmail(),
			resp.DomainInfo.Data,
			resp.DomainInfo.GetStatus(),
			resp.Configuration.GetWorkflowExecutionRetentionPeriodInDays(),
			resp.Configuration.GetEmitMetric(),
			resp.ReplicationConfiguration.GetActiveClusterName(),
			clustersToString(resp.ReplicationConfiguration.Clusters))
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

	ctx, cancel := newContext()
	defer cancel()
	history, err := GetHistory(ctx, wfClient, wid, rid)
	if err != nil {
		ErrorAndExit(fmt.Sprintf("Failed to get history on workflow id: %s, run id: %s.", wid, rid), err)
	}

	if printFully { // dump everything
		for _, e := range history.Events {
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
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	tasklist := getRequiredOption(c, FlagTaskList)
	workflowType := getRequiredOption(c, FlagWorkflowType)
	et := c.Int(FlagExecutionTimeout)
	if et == 0 {
		ErrorAndExit(FlagExecutionTimeout+"is required", nil)
	}
	dt := c.Int(FlagDecisionTimeout)
	wid := c.String(FlagWorkflowID)
	if len(wid) == 0 {
		wid = uuid.New()
	}

	input := processJSONInput(c)

	tcCtx, cancel := newContext()
	defer cancel()

	resp, err := serviceClient.StartWorkflowExecution(tcCtx, &s.StartWorkflowExecutionRequest{
		RequestId:  common.StringPtr(uuid.New()),
		Domain:     common.StringPtr(domain),
		WorkflowId: common.StringPtr(wid),
		WorkflowType: &s.WorkflowType{
			Name: common.StringPtr(workflowType),
		},
		TaskList: &s.TaskList{
			Name: common.StringPtr(tasklist),
		},
		Input:                               []byte(input),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(int32(et)),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(int32(dt)),
		Identity:                            common.StringPtr(getCliIdentity()),
	})

	if err != nil {
		ErrorAndExit("Failed to create workflow.", err)
	} else {
		fmt.Printf("Started Workflow Id: %s, run Id: %s\n", wid, resp.GetRunId())
	}
}

// RunWorkflow starts a new workflow execution and print workflow progress and result
func RunWorkflow(c *cli.Context) {
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	tasklist := getRequiredOption(c, FlagTaskList)
	workflowType := getRequiredOption(c, FlagWorkflowType)
	et := c.Int(FlagExecutionTimeout)
	if et == 0 {
		ErrorAndExit(FlagExecutionTimeout+" is required", nil)
	}
	dt := c.Int(FlagDecisionTimeout)
	wid := c.String(FlagWorkflowID)
	if len(wid) == 0 {
		wid = uuid.New()
	}

	input := processJSONInput(c)

	contextTimeout := defaultContextTimeoutForLongPoll
	if c.IsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.Int(FlagContextTimeout)) * time.Second
	}
	tcCtx, cancel := newContextForLongPoll(contextTimeout)
	defer cancel()

	resp, err := serviceClient.StartWorkflowExecution(tcCtx, &s.StartWorkflowExecutionRequest{
		RequestId:  common.StringPtr(uuid.New()),
		Domain:     common.StringPtr(domain),
		WorkflowId: common.StringPtr(wid),
		WorkflowType: &s.WorkflowType{
			Name: common.StringPtr(workflowType),
		},
		TaskList: &s.TaskList{
			Name: common.StringPtr(tasklist),
		},
		Input:                               []byte(input),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(int32(et)),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(int32(dt)),
		Identity:                            common.StringPtr(getCliIdentity()),
	})

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
		{"Task List", tasklist},
		{"Args", truncate(input)}, // in case of large input
	}
	table.SetBorder(false)
	table.SetColumnSeparator(":")
	table.AppendBulk(executionData) // Add Bulk Data
	table.Render()

	printWorkflowProgress(c, wid, resp.GetRunId())
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

	contextTimeout := defaultContextTimeoutForLongPoll
	if c.IsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.Int(FlagContextTimeout)) * time.Second
	}
	tcCtx, cancel := newContextForLongPoll(contextTimeout)
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

	ctx, cancel := newContext()
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

	ctx, cancel := newContext()
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
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	name := getRequiredOption(c, FlagName)
	input := processJSONInput(c)

	tcCtx, cancel := newContext()
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
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	input := processJSONInput(c)

	tcCtx, cancel := newContext()
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
	pageSize := c.Int(FlagPageSize)

	table := createTableForListWorkflow(false)
	prepareTable := listWorkflow(c, table)

	if !more { // default mode only show one page items
		prepareTable(nil)
		table.Render()
	} else { // require input Enter to view next page
		var resultSize int
		var nextPageToken []byte
		for {
			nextPageToken, resultSize = prepareTable(nextPageToken)
			table.Render()
			table.ClearRows()

			if resultSize < pageSize {
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
	table := createTableForListWorkflow(true)
	prepareTable := listWorkflow(c, table)
	var resultSize int
	var nextPageToken []byte
	for {
		nextPageToken, resultSize = prepareTable(nextPageToken)
		if resultSize < defaultPageSizeForList {
			break
		}
	}
	table.Render()
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
		ErrorAndExit("Parameter workflow_id is required.", nil)
	}
	wid := c.Args().First()
	rid := ""
	if c.NArg() >= 2 {
		rid = c.Args().Get(1)
	}

	describeWorkflowHelper(c, wid, rid)
}

func describeWorkflowHelper(c *cli.Context, wid, rid string) {
	wfClient := getWorkflowClient(c)

	printRawTime := c.Bool(FlagPrintRawTime) // default show datetime instead of raw time

	ctx, cancel := newContext()
	defer cancel()

	resp, err := wfClient.DescribeWorkflowExecution(ctx, wid, rid)
	if err != nil {
		ErrorAndExit("Describe workflow execution failed", err)
	}
	var o interface{}
	if printRawTime {
		o = resp
	} else {
		o = convertDescribeWorkflowExecutionResponse(resp)
	}
	prettyPrintJSONObject(o)
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
	ExecutionConfiguration *s.WorkflowExecutionConfiguration
	WorkflowExecutionInfo  workflowExecutionInfo
	PendingActivities      []*pendingActivityInfo
}

// workflowExecutionInfo has same fields as shared.WorkflowExecutionInfo, but has datetime instead of raw time
type workflowExecutionInfo struct {
	Execution     *s.WorkflowExecution
	Type          *s.WorkflowType
	StartTime     *string // change from *int64
	CloseTime     *string // change from *int64
	CloseStatus   *s.WorkflowExecutionCloseStatus
	HistoryLength *int64
}

// pendingActivityInfo has same fields as shared.PendingActivityInfo, but different field type for better display
type pendingActivityInfo struct {
	ActivityID             *string
	ActivityType           *s.ActivityType
	State                  *s.PendingActivityState
	HeartbeatDetails       *string // change from byte[]
	LastHeartbeatTimestamp *string // change from *int64
}

func convertDescribeWorkflowExecutionResponse(resp *s.DescribeWorkflowExecutionResponse) *describeWorkflowExecutionResponse {
	info := resp.WorkflowExecutionInfo
	executionInfo := workflowExecutionInfo{
		Execution:     info.Execution,
		Type:          info.Type,
		StartTime:     common.StringPtr(convertTime(info.GetStartTime(), false)),
		CloseTime:     common.StringPtr(convertTime(info.GetCloseTime(), false)),
		CloseStatus:   info.CloseStatus,
		HistoryLength: info.HistoryLength,
	}
	var pendingActs []*pendingActivityInfo
	var tmpAct *pendingActivityInfo
	for _, pa := range resp.PendingActivities {
		tmpAct = &pendingActivityInfo{
			ActivityID:             pa.ActivityID,
			ActivityType:           pa.ActivityType,
			State:                  pa.State,
			HeartbeatDetails:       common.StringPtr(string(pa.HeartbeatDetails)),
			LastHeartbeatTimestamp: common.StringPtr(convertTime(pa.GetLastHeartbeatTimestamp(), false)),
		}
		pendingActs = append(pendingActs, tmpAct)
	}

	return &describeWorkflowExecutionResponse{
		ExecutionConfiguration: resp.ExecutionConfiguration,
		WorkflowExecutionInfo:  executionInfo,
		PendingActivities:      pendingActs,
	}
}

func createTableForListWorkflow(listAll bool) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader([]string{"Workflow Type", "Workflow ID", "Run ID", "Start Time", "End Time"})
	if !listAll { // color is only friendly to ANSI terminal
		table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue)
	}
	table.SetHeaderLine(false)
	return table
}

func listWorkflow(c *cli.Context, table *tablewriter.Table) func([]byte) ([]byte, int) {
	wfClient := getWorkflowClient(c)

	queryOpen := c.Bool(FlagOpen)
	earliestTime := parseTime(c.String(FlagEarliestTime), 0)
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UnixNano())
	workflowID := c.String(FlagWorkflowID)
	workflowType := c.String(FlagWorkflowType)
	printRawTime := c.Bool(FlagPrintRawTime)
	printDateTime := c.Bool(FlagPrintDateTime)
	pageSize := c.Int(FlagPageSize)
	if pageSize <= 0 {
		pageSize = defaultPageSizeForList
	}
	timeout := time.Duration(c.Int(FlagContextTimeout)) * time.Second

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
		if queryOpen {
			result, nextPageToken = listOpenWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, next, timeout)
		} else {
			result, nextPageToken = listClosedWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, workflowStatus, next, timeout)
		}

		for _, e := range result {
			var startTime, closeTime string
			if printRawTime {
				startTime = fmt.Sprintf("%d", e.GetStartTime())
				closeTime = fmt.Sprintf("%d", e.GetCloseTime())
			} else {
				startTime = convertTime(e.GetStartTime(), !printDateTime)
				closeTime = convertTime(e.GetCloseTime(), !printDateTime)
			}
			table.Append([]string{trimWorkflowType(e.Type.GetName()), e.Execution.GetWorkflowId(), e.Execution.GetRunId(), startTime, closeTime})
		}

		return nextPageToken, len(result)
	}
	return prepareTable
}

func listOpenWorkflow(client client.Client, pageSize int, earliestTime, latestTime int64, workflowID, workflowType string,
	nextPageToken []byte, timeout time.Duration) ([]*s.WorkflowExecutionInfo, []byte) {

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

	ctx, cancel := newContextForLongPoll(timeout)
	defer cancel()
	response, err := client.ListOpenWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to list open workflow.", err)
	}
	return response.Executions, response.NextPageToken
}

func listClosedWorkflow(client client.Client, pageSize int, earliestTime, latestTime int64, workflowID, workflowType string,
	workflowStatus s.WorkflowExecutionCloseStatus, nextPageToken []byte, timeout time.Duration) ([]*s.WorkflowExecutionInfo, []byte) {

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

	ctx, cancel := newContextForLongPoll(timeout)
	defer cancel()
	response, err := client.ListClosedWorkflow(ctx, request)
	if err != nil {
		ErrorAndExit("Failed to list closed workflow.", err)
	}
	return response.Executions, response.NextPageToken
}

// DescribeTaskList show pollers info of a given tasklist
func DescribeTaskList(c *cli.Context) {
	wfClient := getWorkflowClient(c)
	taskList := getRequiredOption(c, FlagTaskList)
	taskListType := strToTaskListType(c.String(FlagTaskListType)) // default type is decision

	ctx, cancel := newContext()
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

// ObserveHistoryWithID show the process of running workflow
func ObserveHistoryWithID(c *cli.Context) {
	if !c.Args().Present() {
		ErrorAndExit("workflow_id is required.", nil)
	}
	wid := c.Args().First()
	rid := ""
	if c.NArg() >= 2 {
		rid = c.Args().Get(1)
	}

	printWorkflowProgress(c, wid, rid)
}

func getDomainClient(c *cli.Context) client.DomainClient {
	service, err := cBuilder.BuildServiceClient(c)
	if err != nil {
		ErrorAndExit("Failed to initialize service client.", err)
	}

	domainClient, err := client.NewDomainClient(service, &client.Options{}), nil
	if err != nil {
		ErrorAndExit("Failed to initialize domain client.", err)
	}
	return domainClient
}

func getWorkflowClient(c *cli.Context) client.Client {
	domain := getRequiredGlobalOption(c, FlagDomain)

	service, err := cBuilder.BuildServiceClient(c)
	if err != nil {
		ErrorAndExit("Failed to initialize service client.", err)
	}

	wfClient, err := client.NewClient(service, domain, &client.Options{}), nil
	if err != nil {
		ErrorAndExit("Failed to initialize workflow client.", err)
	}

	return wfClient
}

func getWorkflowServiceClient(c *cli.Context) workflowserviceclient.Interface {
	client, err := cBuilder.BuildServiceClient(c)
	if err != nil {
		ErrorAndExit("Failed to initialize service client.", err)
	}

	return client
}

func getRequiredOption(c *cli.Context, optionName string) string {
	value := c.String(optionName)
	if len(value) == 0 {
		ErrorAndExit(fmt.Sprintf("%s is required", optionName), nil)
	}
	return value
}

func getRequiredGlobalOption(c *cli.Context, optionName string) string {
	value := c.GlobalString(optionName)
	if len(value) == 0 {
		ErrorAndExit(fmt.Sprintf("%s is required", optionName), nil)
	}
	return value
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

func newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*5)
}

func newContextForLongPoll(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// process and validate input provided through cmd or file
func processJSONInput(c *cli.Context) string {
	var input string
	if c.IsSet(FlagInput) {
		input = c.String(FlagInput)
	} else if c.IsSet(FlagInputFile) {
		inputFile := c.String(FlagInputFile)
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

func clustersToString(clusters []*s.ClusterReplicationConfiguration) string {
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
