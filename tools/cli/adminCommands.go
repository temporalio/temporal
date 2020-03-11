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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	commonproto "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/auth"
	"github.com/temporalio/temporal/common/codec"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/persistence"
	cassp "github.com/temporalio/temporal/common/persistence/cassandra"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/tools/cassandra"
)

const maxEventID = 9999

// AdminShowWorkflow shows history
func AdminShowWorkflow(c *cli.Context) {
	tid := c.String(FlagTreeID)
	bid := c.String(FlagBranchID)
	sid := c.Int(FlagShardID)
	outputFileName := c.String(FlagOutputFilename)

	session := connectToCassandra(c)
	serializer := persistence.NewPayloadSerializer()
	var history []*serialization.DataBlob
	if len(tid) != 0 {
		histV2 := cassp.NewHistoryV2PersistenceFromSession(session, loggerimpl.NewNopLogger())
		resp, err := histV2.ReadHistoryBranch(&persistence.InternalReadHistoryBranchRequest{
			TreeID:    primitives.MustParseUUID(tid),
			BranchID:  primitives.MustParseUUID(bid),
			MinNodeID: 1,
			MaxNodeID: maxEventID,
			PageSize:  maxEventID,
			ShardID:   sid,
		})
		if err != nil {
			ErrorAndExit("ReadHistoryBranch err", err)
		}

		history = resp.History
	} else {
		ErrorAndExit("need to specify TreeID/BranchID/ShardID", nil)
	}

	if len(history) == 0 {
		ErrorAndExit("no events", nil)
	}
	allEvents := &commonproto.History{}
	totalSize := 0
	for idx, b := range history {
		totalSize += len(b.Data)
		fmt.Printf("======== batch %v, blob len: %v ======\n", idx+1, len(b.Data))
		historyBatchThrift, err := serializer.DeserializeBatchEvents(b)
		if err != nil {
			ErrorAndExit("DeserializeBatchEvents err", err)
		}
		historyBatch := historyBatchThrift
		allEvents.Events = append(allEvents.Events, historyBatch...)
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(historyBatch)
		if err != nil {
			ErrorAndExit("EncodeHistoryEvents err", err)
		}
		fmt.Println(string(data))
	}
	fmt.Printf("======== total batches %v, total blob len: %v ======\n", len(history), totalSize)

	if outputFileName != "" {
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(allEvents.Events)
		if err != nil {
			ErrorAndExit("Failed to serialize history data.", err)
		}
		if err := ioutil.WriteFile(outputFileName, data, 0777); err != nil {
			ErrorAndExit("Failed to export history data file.", err)
		}
	}
}

// AdminDescribeWorkflow describe a new workflow execution for admin
func AdminDescribeWorkflow(c *cli.Context) {

	resp := describeMutableState(c)
	prettyPrintJSONObject(resp)

	if resp != nil {
		msStr := resp.GetMutableStateInDatabase()
		ms := persistence.WorkflowMutableState{}
		// TODO: this won't work for some cases because json.Unmarshal can't be used for proto object
		// Proper refactoring is required here: resp.GetMutableStateInDatabase() should return proto object.
		err := json.Unmarshal([]byte(msStr), &ms)
		if err != nil {
			ErrorAndExit("json.Unmarshal err", err)
		}
		currentBranchToken := ms.ExecutionInfo.BranchToken
		if ms.VersionHistories != nil {
			// if VersionHistories is set, then all branch infos are stored in VersionHistories
			currentVersionHistory, err := ms.VersionHistories.GetCurrentVersionHistory()
			if err != nil {
				ErrorAndExit("ms.VersionHistories.GetCurrentVersionHistory err", err)
			}
			currentBranchToken = currentVersionHistory.GetBranchToken()
		}

		branchInfo := persistenceblobs.HistoryBranch{}
		err = branchInfo.Unmarshal(currentBranchToken)
		if err != nil {
			ErrorAndExit("failed to unmarshal current branch token from proto", err)
		}
		prettyPrintJSONObject(branchInfo)
		if ms.ExecutionInfo.AutoResetPoints != nil {
			fmt.Println("auto-reset-points:")
			for _, p := range ms.ExecutionInfo.AutoResetPoints.Points {
				createT := time.Unix(0, p.GetCreatedTimeNano())
				expireT := time.Unix(0, p.GetExpiringTimeNano())
				fmt.Println(p.GetBinaryChecksum(), p.GetRunId(), p.GetFirstDecisionCompletedId(), p.GetResettable(), createT, expireT)
			}
		}
	}
}

func describeMutableState(c *cli.Context) *adminservice.DescribeWorkflowExecutionResponse {
	adminClient := cFactory.AdminClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeWorkflowExecution(ctx, &adminservice.DescribeWorkflowExecutionRequest{
		Domain: domain,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		ErrorAndExit("Get workflow mutableState failed", err)
	}
	return resp
}

// AdminDeleteWorkflow delete a workflow execution for admin
func AdminDeleteWorkflow(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	resp := describeMutableState(c)
	msStr := resp.GetMutableStateInDatabase()
	ms := persistence.WorkflowMutableState{}
	err := json.Unmarshal([]byte(msStr), &ms)
	if err != nil {
		ErrorAndExit("json.Unmarshal err", err)
	}
	domainID := ms.ExecutionInfo.DomainID
	skipError := c.Bool(FlagSkipErrorMode)
	session := connectToCassandra(c)
	shardID := resp.GetShardId()
	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		ErrorAndExit("strconv.Atoi(shardID) err", err)
	}

	branchTokens := [][]byte{ms.ExecutionInfo.BranchToken}
	if ms.VersionHistories != nil {
		// if VersionHistories is set, then all branch infos are stored in VersionHistories
		branchTokens = [][]byte{}
		for _, versionHistory := range ms.VersionHistories.ToProto().Histories {
			branchTokens = append(branchTokens, versionHistory.BranchToken)
		}
	}

	for _, branchToken := range branchTokens {
		branchInfo, err := serialization.HistoryBranchFromBlob(branchToken, common.EncodingTypeProto3.String())
		if err != nil {
			ErrorAndExit("thriftrwEncoder.Decode err", err)
		}
		fmt.Println("deleting history events for ...")
		prettyPrintJSONObject(branchInfo)
		histV2 := cassp.NewHistoryV2PersistenceFromSession(session, loggerimpl.NewNopLogger())
		err = histV2.DeleteHistoryBranch(&persistence.InternalDeleteHistoryBranchRequest{
			BranchInfo: branchInfo,
			ShardID:    shardIDInt,
		})
		if err != nil {
			if skipError {
				fmt.Println("failed to delete history, ", err)
			} else {
				ErrorAndExit("DeleteHistoryBranch err", err)
			}
		}
	}

	exeStore, _ := cassp.NewWorkflowExecutionPersistence(shardIDInt, session, loggerimpl.NewNopLogger())
	req := &persistence.DeleteWorkflowExecutionRequest{
		DomainID:   domainID,
		WorkflowID: wid,
		RunID:      rid,
	}

	err = exeStore.DeleteWorkflowExecution(req)
	if err != nil {
		if skipError {
			fmt.Println("delete mutableState row failed, ", err)
		} else {
			ErrorAndExit("delete mutableState row failed", err)
		}
	}
	fmt.Println("delete mutableState row successfully")

	deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
		DomainID:   domainID,
		WorkflowID: wid,
		RunID:      rid,
	}

	err = exeStore.DeleteCurrentWorkflowExecution(deleteCurrentReq)
	if err != nil {
		if skipError {
			fmt.Println("delete current row failed, ", err)
		} else {
			ErrorAndExit("delete current row failed", err)
		}
	}
	fmt.Println("delete current row successfully")
}

func readOneRow(query *gocql.Query) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	err := query.MapScan(result)
	return result, err
}

func connectToCassandra(c *cli.Context) *gocql.Session {
	host := getRequiredOption(c, FlagDBAddress)
	if !c.IsSet(FlagDBPort) {
		ErrorAndExit("cassandra port is required", nil)
	}

	cassandraConfig := &config.Cassandra{
		Hosts:    host,
		Port:     c.Int(FlagDBPort),
		User:     c.String(FlagUsername),
		Password: c.String(FlagPassword),
		Keyspace: getRequiredOption(c, FlagKeyspace),
	}
	if c.Bool(FlagEnableTLS) {
		cassandraConfig.TLS = &auth.TLS{
			Enabled:                true,
			CertFile:               c.String(FlagTLSCertPath),
			KeyFile:                c.String(FlagTLSKeyPath),
			CaFile:                 c.String(FlagTLSCaPath),
			EnableHostVerification: c.Bool(FlagTLSEnableHostVerification),
		}
	}

	clusterCfg, err := cassandra.NewCassandraCluster(cassandraConfig, 10)
	clusterCfg.SerialConsistency = gocql.LocalSerial
	clusterCfg.NumConns = 20
	if err != nil {
		ErrorAndExit("connect to Cassandra failed", err)
	}
	session, err := clusterCfg.CreateSession()
	if err != nil {
		ErrorAndExit("connect to Cassandra failed", err)
	}
	return session
}

// AdminGetDomainIDOrName map domain
func AdminGetDomainIDOrName(c *cli.Context) {
	domainID := c.String(FlagDomainID)
	domainName := c.String(FlagDomain)
	if len(domainID) == 0 && len(domainName) == 0 {
		ErrorAndExit("Need either domainName or domainID", nil)
	}

	session := connectToCassandra(c)

	if len(domainID) > 0 {
		tmpl := "select domain from domains where id = ? "
		query := session.Query(tmpl, domainID)
		res, err := readOneRow(query)
		if err != nil {
			ErrorAndExit("readOneRow", err)
		}
		domain := res["domain"].(map[string]interface{})
		domainName := domain["name"].(string)
		fmt.Printf("domainName for domainID %v is %v \n", domainID, domainName)
	} else {
		tmpl := "select domain from domains_by_name where name = ?"
		tmplV2 := "select domain from domains_by_name_v2 where domains_partition=0 and name = ?"

		query := session.Query(tmpl, domainName)
		res, err := readOneRow(query)
		if err != nil {
			fmt.Printf("v1 return error: %v , trying v2...\n", err)

			query := session.Query(tmplV2, domainName)
			res, err := readOneRow(query)
			if err != nil {
				ErrorAndExit("readOneRow for v2", err)
			}
			domain := res["domain"].(map[string]interface{})
			domainID := domain["id"].(gocql.UUID).String()
			fmt.Printf("domainID for domainName %v is %v \n", domainName, domainID)
		} else {
			domain := res["domain"].(map[string]interface{})
			domainID := domain["id"].(gocql.UUID).String()
			fmt.Printf("domainID for domainName %v is %v \n", domainName, domainID)
		}
	}
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	numberOfShards := c.Int(FlagNumberOfShards)

	if numberOfShards <= 0 {
		ErrorAndExit("numberOfShards is required", nil)
		return
	}
	shardID := common.WorkflowIDToHistoryShard(wid, numberOfShards)
	fmt.Printf("ShardID for workflowID: %v is %v \n", wid, shardID)
}

// AdminRemoveTask describes history host
func AdminRemoveTask(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	sid := getRequiredIntOption(c, FlagShardID)
	taskID := getRequiredInt64Option(c, FlagRemoveTaskID)
	typeID := getRequiredIntOption(c, FlagRemoveTypeID)

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.RemoveTaskRequest{}

	req.ShardID = int32(sid)
	req.TaskID = taskID
	req.Type = int32(typeID)

	_, err := adminClient.RemoveTask(ctx, req)
	if err != nil {
		ErrorAndExit("Remove task has failed", err)
	}
}

// AdminShardManagement describes history host
func AdminShardManagement(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)
	sid := getRequiredIntOption(c, FlagShardID)

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.CloseShardRequest{}
	req.ShardID = int32(sid)

	_, err := adminClient.CloseShard(ctx, req)
	if err != nil {
		ErrorAndExit("Close shard task has failed", err)
	}
}

// AdminDescribeHistoryHost describes history host
func AdminDescribeHistoryHost(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	wid := c.String(FlagWorkflowID)
	sid := c.Int(FlagShardID)
	addr := c.String(FlagHistoryAddress)
	printFully := c.Bool(FlagPrintFullyDetail)

	if len(wid) == 0 && !c.IsSet(FlagShardID) && len(addr) == 0 {
		ErrorAndExit("at least one of them is required to provide to lookup host: workflowID, shardID and host address", nil)
		return
	}

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.DescribeHistoryHostRequest{}
	if len(wid) > 0 {
		req.ExecutionForHost = &commonproto.WorkflowExecution{WorkflowId: wid}
	}
	if c.IsSet(FlagShardID) {
		req.ShardIdForHost = int32(sid)
	}
	if len(addr) > 0 {
		req.HostAddress = addr
	}

	resp, err := adminClient.DescribeHistoryHost(ctx, req)
	if err != nil {
		ErrorAndExit("Describe history host failed", err)
	}

	if !printFully {
		resp.ShardIDs = nil
	}
	prettyPrintJSONObject(resp)
}

// AdminRefreshWorkflowTasks refreshes all the tasks of a workflow
func AdminRefreshWorkflowTasks(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	_, err := adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		Domain: domain,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		ErrorAndExit("Refresh workflow task failed", err)
	} else {
		fmt.Println("Refresh workflow task succeeded.")
	}
}
