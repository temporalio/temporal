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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/urfave/cli"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	cassp "go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/tools/cassandra"
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
			TreeID:    tid,
			BranchID:  bid,
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
		ErrorAndExit("need to specify TreeId/BranchId/ShardId", nil)
	}

	if len(history) == 0 {
		ErrorAndExit("no events", nil)
	}
	allEvents := &historypb.History{}
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
		if err := ioutil.WriteFile(outputFileName, data, 0666); err != nil {
			ErrorAndExit("Failed to export history data file.", err)
		}
	}
}

// AdminDescribeWorkflow describe a new workflow execution for admin
func AdminDescribeWorkflow(c *cli.Context) {
	identJSON := func(s string) string {
		if s == "" {
			return "<empty>"
		}

		var data interface{}
		err := json.Unmarshal([]byte(s), &data)
		if err != nil {
			return s
		}

		b, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return s
		}
		return string(b)
	}

	resp := describeMutableState(c)
	fmt.Printf("Cache Mutable State:\n%s\n", identJSON(resp.GetCacheMutableState()))
	fmt.Printf("Database Mutable State:\n%s\n", identJSON(resp.GetDatabaseMutableState()))

	output := struct {
		TreeID      string
		BranchID    string
		ShardID     string
		HistoryAddr string
	}{
		resp.GetTreeId(),
		resp.GetBranchId(),
		resp.GetShardId(),
		resp.GetHistoryAddr(),
	}
	prettyPrintJSONObject(output)

	// This commented out block should be restored when MutableState is passed as proto object.

	// if resp != nil {
	// 	msStr := resp.GetDatabaseMutableState()
	// 	ms := persistence.WorkflowMutableState{}
	// 	// TODO: this won't work for some cases because json.Unmarshal can't be used for proto object
	// 	// Proper refactoring is required here: resp.GetDatabaseMutableState() should return proto object.
	// 	err := json.Unmarshal([]byte(msStr), &ms)
	// 	if err != nil {
	// 		ErrorAndExit("json.Unmarshal err", err)
	// 	}
	// 	currentBranchToken := ms.ExecutionInfo.EventBranchToken
	// 	if ms.VersionHistories != nil {
	// 		// if VersionHistories is set, then all branch infos are stored in VersionHistories
	// 		currentVersionHistory, err := ms.VersionHistories.GetCurrentVersionHistory()
	// 		if err != nil {
	// 			ErrorAndExit("ms.VersionHistories.GetCurrentVersionHistory err", err)
	// 		}
	// 		currentBranchToken = currentVersionHistory.GetBranchToken()
	// 	}
	//
	// 	branchInfo := persistenceblobs.HistoryBranch{}
	// 	err = branchInfo.Unmarshal(currentBranchToken)
	// 	if err != nil {
	// 		ErrorAndExit("failed to unmarshal current branch token from proto", err)
	// 	}
	// 	prettyPrintJSONObject(branchInfo)
	// 	if ms.ExecutionInfo.AutoResetPoints != nil {
	// 		fmt.Println("auto-reset-points:")
	// 		for _, p := range ms.ExecutionInfo.AutoResetPoints.Points {
	// 			createT := timestamp.TimeValue(p.GetCreateTime())
	// 			expireT := timestamp.TimeValue(p.GetExpireTime())
	// 			fmt.Println(p.GetBinaryChecksum(), p.GetRunId(), p.GetFirstWorkflowTaskCompletedId(), p.GetResettable(), createT, expireT)
	// 		}
	// 	}
	// }
}

func describeMutableState(c *cli.Context) *adminservice.DescribeWorkflowExecutionResponse {
	adminClient := cFactory.AdminClient(c)

	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeWorkflowExecution(ctx, &adminservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		ErrorAndExit("Get workflow mutableState failed", err)
	}
	return resp
}

// AdminListNamespaces outputs a list of all namespaces
func AdminListNamespaces(c *cli.Context) {
	pFactory := CreatePersistenceFactory(c)
	metadataManager, err := pFactory.NewMetadataManager()
	if err != nil {
		ErrorAndExit("Failed to initialize metadata manager", err)
	}

	req := &persistence.ListNamespacesRequest{
		PageSize: c.Int(FlagPageSize),
	}
	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		req.NextPageToken = paginationToken
		response, err := metadataManager.ListNamespaces(req)
		if err != nil {
			return nil, nil, err
		}
		token := response.NextPageToken

		var items []interface{}
		for _, task := range response.Namespaces {
			items = append(items, task)
		}
		return items, token, nil
	}
	paginate(c, paginationFunc)
}

// AdminDeleteWorkflow delete a workflow execution for admin
func AdminDeleteWorkflow(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	resp := describeMutableState(c)
	// TODO: this is temporary solution for JSON version of WorkflowMutableState.
	// Proper refactoring is required here: resp.GetDatabaseMutableState() should return proto object.
	msStr := resp.GetDatabaseMutableState()
	ms := map[string]interface{}{}
	err := json.Unmarshal([]byte(msStr), &ms)
	if err != nil {
		ErrorAndExit("json.Unmarshal err", err)
	}
	namespaceID := ms["ExecutionInfo"].(map[string]interface{})["NamespaceId"].(string)
	skipError := c.Bool(FlagSkipErrorMode)
	session := connectToCassandra(c)
	shardID := resp.GetShardId()
	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		ErrorAndExit("strconv.Atoi(shardID) err", err)
	}

	var branchTokens [][]byte
	if vhs, ok := ms["VersionHistories"]; ok && vhs != nil {
		// if VersionHistories is set, then all branch infos are stored in VersionHistories
		branchTokens = [][]byte{}
		for i, history := range ms["VersionHistories"].(map[string]interface{})["Histories"].([]interface{}) {
			versionHistory := history.(map[string]interface{})
			branchToken := versionHistory["BranchToken"].(string)
			branchTokenBytes, err := base64.StdEncoding.DecodeString(branchToken)
			if err != nil {
				ErrorAndExit(fmt.Sprintf("unable to decode BranchToken for history item %d", i), err)
			}

			branchTokens = append(branchTokens, branchTokenBytes)
		}
	} else {
		eventBranchToken := ms["ExecutionInfo"].(map[string]interface{})["EventBranchToken"].(string)
		eventBranchTokenBytes, err := base64.StdEncoding.DecodeString(eventBranchToken)
		if err != nil {
			ErrorAndExit("unable to decode EventBranchToken", err)
		}
		branchTokens = append(branchTokens, eventBranchTokenBytes)
	}

	for _, branchToken := range branchTokens {
		branchInfo, err := serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
		if err != nil {
			ErrorAndExit("HistoryBranchFromBlob decoder err", err)
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
		NamespaceID: namespaceID,
		WorkflowID:  wid,
		RunID:       rid,
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
		NamespaceID: namespaceID,
		WorkflowID:  wid,
		RunID:       rid,
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
	if err != nil {
		ErrorAndExit("connect to Cassandra failed", err)
	}
	clusterCfg.SerialConsistency = gocql.LocalSerial
	clusterCfg.NumConns = 20
	clusterCfg.PoolConfig.HostSelectionPolicy = nil

	session, err := clusterCfg.CreateSession()
	if err != nil {
		ErrorAndExit("connect to Cassandra failed", err)
	}
	return session
}

// AdminGetNamespaceIDOrName map namespace
func AdminGetNamespaceIDOrName(c *cli.Context) {
	namespaceID := c.String(FlagNamespaceID)
	namespace := c.String(FlagNamespace)
	if len(namespaceID) == 0 && len(namespace) == 0 {
		ErrorAndExit("Need either namespace or namespaceId", nil)
	}

	session := connectToCassandra(c)

	if len(namespaceID) > 0 {
		tmpl := "select namespace from namespaces where id = ? "
		query := session.Query(tmpl, namespaceID)
		res, err := readOneRow(query)
		if err != nil {
			ErrorAndExit("readOneRow", err)
		}
		namespaceName := res["name"].(string)
		fmt.Printf("namespace for namespaceId %v is %v \n", namespaceID, namespaceName)
	} else {
		tmpl := "select namespace from namespaces_by_name where name = ?"
		tmplV2 := "select namespace from namespaces where namespaces_partition=0 and name = ?"

		query := session.Query(tmpl, namespace)
		res, err := readOneRow(query)
		if err != nil {
			fmt.Printf("v1 return error: %v , trying v2...\n", err)

			query := session.Query(tmplV2, namespace)
			res, err := readOneRow(query)
			if err != nil {
				ErrorAndExit("readOneRow for v2", err)
			}
			namespace := res["namespace"].(map[string]interface{})
			namespaceID := namespace["id"].(gocql.UUID).String()
			fmt.Printf("namespaceId for namespace %v is %v \n", namespace, namespaceID)
		} else {
			namespace := res["namespace"].(map[string]interface{})
			namespaceID := namespace["id"].(gocql.UUID).String()
			fmt.Printf("namespaceId for namespace %v is %v \n", namespace, namespaceID)
		}
	}
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) {
	namespaceID := getRequiredOption(c, FlagNamespaceID)
	wid := getRequiredOption(c, FlagWorkflowID)
	numberOfShards := c.Int(FlagNumberOfShards)

	if numberOfShards <= 0 {
		ErrorAndExit("numberOfShards is required", nil)
		return
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID, wid, numberOfShards)
	fmt.Printf("ShardId for namespace, workflowId: %v, %v is %v \n", namespaceID, wid, shardID)
}

// AdminDescribeTask outputs the details of a task given Task Id, Task Type, Shard Id and Visibility Timestamp
func AdminDescribeTask(c *cli.Context) {
	sid := getRequiredIntOption(c, FlagShardID)
	tid := getRequiredIntOption(c, FlagTaskID)
	categoryInt, err := stringToEnum(c.String(FlagTaskType), enumsspb.TaskCategory_value)
	if err != nil {
		ErrorAndExit("Failed to parse Task Type", err)
	}
	category := enumsspb.TaskCategory(categoryInt)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		ErrorAndExit(fmt.Sprintf("Task type %s is currently not supported", category), nil)
	}

	pFactory := CreatePersistenceFactory(c)
	executionManager, err := pFactory.NewExecutionManager(sid)
	if err != nil {
		ErrorAndExit("Failed to initialize execution manager", err)
	}

	if category == enumsspb.TASK_CATEGORY_TIMER {
		vis := getRequiredInt64Option(c, FlagTaskVisibilityTimestamp)
		req := &persistence.GetTimerTaskRequest{ShardID: int32(sid), TaskID: int64(tid), VisibilityTimestamp: time.Unix(0, vis).UTC()}
		task, err := executionManager.GetTimerTask(req)
		if err != nil {
			ErrorAndExit("Failed to get Timer Task", err)
		}
		prettyPrintJSONObject(task)
	} else if category == enumsspb.TASK_CATEGORY_REPLICATION {
		req := &persistence.GetReplicationTaskRequest{ShardID: int32(sid), TaskID: int64(tid)}
		task, err := executionManager.GetReplicationTask(req)
		if err != nil {
			ErrorAndExit("Failed to get Replication Task", err)
		}
		prettyPrintJSONObject(task)
	} else if category == enumsspb.TASK_CATEGORY_TRANSFER {
		req := &persistence.GetTransferTaskRequest{ShardID: int32(sid), TaskID: int64(tid)}
		task, err := executionManager.GetTransferTask(req)
		if err != nil {
			ErrorAndExit("Failed to get Transfer Task", err)
		}
		prettyPrintJSONObject(task)
	} else {
		ErrorAndExit("Failed to describe task", fmt.Errorf("Unrecognized task type, task_type=%v", category))
	}
}

// AdminListTasks outputs a list of a tasks for given Shard and Task Type
func AdminListTasks(c *cli.Context) {
	sid := getRequiredIntOption(c, FlagShardID)
	categoryInt, err := stringToEnum(c.String(FlagTaskType), enumsspb.TaskCategory_value)
	if err != nil {
		ErrorAndExit("Failed to parse Task Type", err)
	}
	category := enumsspb.TaskCategory(categoryInt)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		ErrorAndExit(fmt.Sprintf("Task type %s is currently not supported", category), nil)
	}

	pFactory := CreatePersistenceFactory(c)
	executionManager, err := pFactory.NewExecutionManager(sid)
	if err != nil {
		ErrorAndExit("Failed to initialize execution manager", err)
	}

	if category == enumsspb.TASK_CATEGORY_TRANSFER {
		req := &persistence.GetTransferTasksRequest{}

		paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
			req.NextPageToken = paginationToken
			response, err := executionManager.GetTransferTasks(req)
			if err != nil {
				return nil, nil, err
			}
			token := response.NextPageToken

			var items []interface{}
			for _, task := range response.Tasks {
				items = append(items, task)
			}
			return items, token, nil
		}
		paginate(c, paginationFunc)
	} else if category == enumsspb.TASK_CATEGORY_TIMER {
		minVis := parseTime(c.String(FlagMinVisibilityTimestamp), time.Time{}, time.Now().UTC())
		maxVis := parseTime(c.String(FlagMaxVisibilityTimestamp), time.Time{}, time.Now().UTC())

		req := &persistence.GetTimerIndexTasksRequest{MinTimestamp: minVis, MaxTimestamp: maxVis}
		paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
			req.NextPageToken = paginationToken
			response, err := executionManager.GetTimerIndexTasks(req)
			if err != nil {
				return nil, nil, err
			}
			token := response.NextPageToken

			var items []interface{}
			for _, task := range response.Timers {
				items = append(items, task)
			}
			return items, token, nil
		}
		paginate(c, paginationFunc)
	} else if category == enumsspb.TASK_CATEGORY_REPLICATION {
		req := &persistence.GetReplicationTasksRequest{}
		paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
			req.NextPageToken = paginationToken
			response, err := executionManager.GetReplicationTasks(req)
			if err != nil {
				return nil, nil, err
			}
			token := response.NextPageToken

			var items []interface{}
			for _, task := range response.Tasks {
				items = append(items, task)
			}
			return items, token, nil
		}
		paginate(c, paginationFunc)
	} else {
		ErrorAndExit("Failed to describe task", fmt.Errorf("Unrecognized task type, task_type=%v", category))
	}
}

// AdminRemoveTask describes history host
func AdminRemoveTask(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)
	shardID := getRequiredIntOption(c, FlagShardID)
	taskID := getRequiredInt64Option(c, FlagTaskID)
	categoryInt, err := stringToEnum(c.String(FlagTaskType), enumsspb.TaskCategory_value)
	if err != nil {
		ErrorAndExit("Failed to parse Task Type", err)
	}
	category := enumsspb.TaskCategory(categoryInt)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		ErrorAndExit(fmt.Sprintf("Task type %s is currently not supported", category), nil)
	}
	var visibilityTimestamp int64
	if category == enumsspb.TASK_CATEGORY_TIMER {
		visibilityTimestamp = getRequiredInt64Option(c, FlagTaskVisibilityTimestamp)
	}

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.RemoveTaskRequest{
		ShardId:        int32(shardID),
		Category:       category,
		TaskId:         taskID,
		VisibilityTime: timestamp.TimePtr(timestamp.UnixOrZeroTime(visibilityTimestamp)),
	}

	_, err = adminClient.RemoveTask(ctx, req)
	if err != nil {
		ErrorAndExit("Remove task has failed", err)
	}
}

// AdminDescribeShard describes shard by shard id
func AdminDescribeShard(c *cli.Context) {
	sid := getRequiredIntOption(c, FlagShardID)
	pFactory := CreatePersistenceFactory(c)
	shardManager, err := pFactory.NewShardManager()

	if err != nil {
		ErrorAndExit("Failed to initialize shard manager", err)
	}

	getShardReq := &persistence.GetShardRequest{ShardID: int32(sid)}
	shard, err := shardManager.GetShard(getShardReq)

	prettyPrintJSONObject(shard)
}

// AdminShardManagement describes history host
func AdminShardManagement(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)
	sid := getRequiredIntOption(c, FlagShardID)

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.CloseShardRequest{}
	req.ShardId = int32(sid)

	_, err := adminClient.CloseShard(ctx, req)
	if err != nil {
		ErrorAndExit("Close shard task has failed", err)
	}
}

// AdminListGossipMembers outputs a list of gossip members
func AdminListGossipMembers(c *cli.Context) {
	roleFlag := c.String(FlagClusterMembershipRole)

	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		ErrorAndExit("Operation DescribeCluster failed.", err)
	}

	members := response.MembershipInfo.Rings
	if roleFlag != primitives.AllServices {
		all := members

		members = members[:0]
		for _, v := range all {
			if roleFlag == v.Role {
				members = append(members, v)
			}
		}
	}

	prettyPrintJSONObject(members)
}

// AdminListClusterMembership outputs a list of cluster membership items
func AdminListClusterMembership(c *cli.Context) {
	roleFlag := c.String(FlagClusterMembershipRole)
	role, err := membership.ServiceNameToServiceTypeEnum(roleFlag)
	if err != nil {
		ErrorAndExit("Failed to map membership role", err)
	}
	// TODO: refactor this: parseTime shouldn't be used for duration.
	heartbeatFlag := parseTime(c.String(FlagEarliestTime), time.Time{}, time.Now().UTC()).UnixNano()
	heartbeat := time.Duration(heartbeatFlag)

	pFactory := CreatePersistenceFactory(c)
	manager, err := pFactory.NewClusterMetadataManager()
	if err != nil {
		ErrorAndExit("Failed to initialize cluster metadata manager", err)
	}

	req := &persistence.GetClusterMembersRequest{
		RoleEquals:          role,
		LastHeartbeatWithin: heartbeat,
	}
	members, err := manager.GetClusterMembers(req)
	if err != nil {
		ErrorAndExit("Failed to get cluster members", err)
	}

	prettyPrintJSONObject(members)
}

// AdminDescribeHistoryHost describes history host
func AdminDescribeHistoryHost(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	wid := c.String(FlagWorkflowID)
	sid := c.Int(FlagShardID)
	addr := c.String(FlagHistoryAddress)
	printFully := c.Bool(FlagPrintFullyDetail)

	if len(wid) == 0 && !c.IsSet(FlagShardID) && len(addr) == 0 {
		ErrorAndExit("at least one of them is required to provide to lookup host: workflowId, shardId and host address", nil)
		return
	}

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.DescribeHistoryHostRequest{}
	if len(wid) > 0 {
		req.WorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: wid}
	}
	if c.IsSet(FlagShardID) {
		req.ShardId = int32(sid)
	}
	if len(addr) > 0 {
		req.HostAddress = addr
	}

	resp, err := adminClient.DescribeHistoryHost(ctx, req)
	if err != nil {
		ErrorAndExit("Describe history host failed", err)
	}

	if !printFully {
		resp.ShardIds = nil
	}
	prettyPrintJSONObject(resp)
}

// AdminRefreshWorkflowTasks refreshes all the tasks of a workflow
func AdminRefreshWorkflowTasks(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	_, err := adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
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
