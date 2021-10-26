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
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/urfave/cli"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

const maxEventID = 9999

// AdminShowWorkflow shows history
func AdminShowWorkflow(c *cli.Context) {
	tid := c.String(FlagTreeID)
	bid := c.String(FlagBranchID)
	sid := int32(c.Int(FlagShardID))
	outputFileName := c.String(FlagOutputFilename)

	session := connectToCassandra(c)
	serializer := serialization.NewSerializer()
	var history []*commonpb.DataBlob
	if len(tid) != 0 {
		histV2 := cassandra.NewHistoryStore(session, log.NewNoopLogger())
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

		for _, node := range resp.Nodes {
			history = append(history, node.Events)
		}
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
		historyBatchThrift, err := serializer.DeserializeEvents(b)
		if err != nil {
			ErrorAndExit("DeserializeEvents err", err)
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
		if err := os.WriteFile(outputFileName, data, 0666); err != nil {
			ErrorAndExit("Failed to export history data file.", err)
		}
	}
}

// AdminDescribeWorkflow describe a new workflow execution for admin
func AdminDescribeWorkflow(c *cli.Context) {
	resp := describeMutableState(c)

	if resp != nil {
		fmt.Println(colorGreen("Cache mutable state:"))
		if resp.GetCacheMutableState() != nil {
			prettyPrintJSONObject(resp.GetCacheMutableState())
		}
		fmt.Println(colorGreen("Database mutable state:"))
		prettyPrintJSONObject(resp.GetDatabaseMutableState())

		fmt.Println(colorGreen("Current branch token:"))
		versionHistories := resp.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
		// if VersionHistories is set, then all branch infos are stored in VersionHistories
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
		if err != nil {
			fmt.Println(colorRed("Unable to get current version history:"), err)
		} else {
			currentBranchToken := persistencespb.HistoryBranch{}
			err := currentBranchToken.Unmarshal(currentVersionHistory.BranchToken)
			if err != nil {
				fmt.Println(colorRed("Unable to unmarshal current branch token:"), err)
			} else {
				prettyPrintJSONObject(currentBranchToken)
			}
		}

		fmt.Printf("History service address: %s\n", resp.GetHistoryAddr())
		fmt.Printf("Shard Id: %s\n", resp.GetShardId())
	}
}

func describeMutableState(c *cli.Context) *adminservice.DescribeMutableStateResponse {
	adminClient := cFactory.AdminClient(c)

	namespace := getRequiredGlobalOption(c, FlagNamespace)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
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

// AdminDeleteWorkflow delete a workflow execution from Cassandra and visibility document from Elasticsearch.
func AdminDeleteWorkflow(c *cli.Context) {
	resp := describeMutableState(c)
	namespaceID := resp.GetDatabaseMutableState().GetExecutionInfo().GetNamespaceId()
	runID := resp.GetDatabaseMutableState().GetExecutionState().GetRunId()

	adminDeleteVisibilityDocument(c, namespaceID)

	session := connectToCassandra(c)
	shardID := resp.GetShardId()
	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		ErrorAndExit("Unable to strconv.Atoi(shardID).", err)
	}
	var branchTokens [][]byte
	versionHistories := resp.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
	// if VersionHistories is set, then all branch infos are stored in VersionHistories
	for _, historyItem := range versionHistories.GetHistories() {
		branchTokens = append(branchTokens, historyItem.GetBranchToken())
	}

	for _, branchToken := range branchTokens {
		branchInfo, err := serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
		if err != nil {
			ErrorAndExit("Unable to HistoryBranchFromBlob.", err)
		}
		fmt.Println("Deleting history events for:")
		prettyPrintJSONObject(branchInfo)
		execStore := cassandra.NewExecutionStore(session, log.NewNoopLogger())
		execMgr := persistence.NewExecutionManager(execStore, log.NewNoopLogger(), dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit))
		err = execMgr.DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
			BranchToken: branchToken,
			ShardID:     int32(shardIDInt),
		})
		if err != nil {
			if c.Bool(FlagSkipErrorMode) {
				fmt.Println("Unable to DeleteHistoryBranch:", err)
			} else {
				ErrorAndExit("Unable to DeleteHistoryBranch.", err)
			}
		}
	}

	exeStore := cassandra.NewExecutionStore(session, log.NewNoopLogger())
	req := &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     int32(shardIDInt),
		NamespaceID: namespaceID,
		WorkflowID:  getRequiredOption(c, FlagWorkflowID),
		RunID:       runID,
	}
	err = exeStore.DeleteWorkflowExecution(req)
	if err != nil {
		if c.Bool(FlagSkipErrorMode) {
			fmt.Printf("Unable to DeleteWorkflowExecution for RunID=%s: %v\n", runID, err)
		} else {
			ErrorAndExit(fmt.Sprintf("Unable to DeleteWorkflowExecution for RunID=%s.", runID), err)
		}
	} else {
		fmt.Printf("DeleteWorkflowExecution for RunID=%s executed successfully.\n", runID)
	}

	deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     int32(shardIDInt),
		NamespaceID: namespaceID,
		WorkflowID:  getRequiredOption(c, FlagWorkflowID),
		RunID:       runID,
	}
	err = exeStore.DeleteCurrentWorkflowExecution(deleteCurrentReq)
	if err != nil {
		if c.Bool(FlagSkipErrorMode) {
			fmt.Printf("Unable to DeleteCurrentWorkflowExecution for RunID=%s: %v\n", runID, err)
		} else {
			ErrorAndExit(fmt.Sprintf("Unable to DeleteCurrentWorkflowExecution for RunID=%s.", runID), err)
		}
	} else {
		fmt.Printf("DeleteCurrentWorkflowExecution for RunID=%s executed successfully.\n", runID)
	}
}

func adminDeleteVisibilityDocument(c *cli.Context, namespaceID string) {
	if !c.IsSet(FlagIndex) {
		prompt("Elasticsearch index name is not specified. Continue without visibility document deletion?", c.GlobalBool(FlagAutoConfirm))
	}

	indexName := getRequiredOption(c, FlagIndex)
	esClient := newESClient(c)

	query := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery(searchattribute.NamespaceID, namespaceID),
			elastic.NewTermQuery(searchattribute.WorkflowID, getRequiredOption(c, FlagWorkflowID)))
	if c.IsSet(FlagRunID) {
		query = query.Filter(elastic.NewTermQuery(searchattribute.RunID, c.String(FlagRunID)))
	}
	searchParams := &esclient.SearchParameters{
		Index:    c.String(FlagIndex),
		Query:    query,
		PageSize: 10000,
	}
	searchResult, err := esClient.Search(context.Background(), searchParams)
	if err != nil {
		if c.Bool(FlagSkipErrorMode) {
			fmt.Println("Unable to search for visibility documents from Elasticsearch:", err)
		} else {
			ErrorAndExit("Unable to search for visibility documents from Elasticsearch.", err)
		}
	}
	fmt.Println("Found", len(searchResult.Hits.Hits), "visibility documents.")
	for _, searchHit := range searchResult.Hits.Hits {
		err := esClient.Delete(context.Background(), indexName, searchHit.Id, math.MaxInt64)
		if err != nil {
			if c.Bool(FlagSkipErrorMode) {
				fmt.Println("Unable to delete visibility document from Elasticsearch:", err)
			} else {
				ErrorAndExit("Unable to delete visibility document from Elasticsearch.", err)
			}
		} else {
			fmt.Println("Visibility document", searchHit.Id, "deleted successfully.")
		}
	}
}

func readOneRow(query gocql.Query) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	err := query.MapScan(result)
	return result, err
}

func connectToCassandra(c *cli.Context) gocql.Session {
	host := getRequiredOption(c, FlagDBAddress)
	port := c.Int(FlagDBPort)

	cassandraConfig := config.Cassandra{
		Hosts:    host,
		Port:     port,
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
			ServerName:             c.String(FlagTLSServerName),
			EnableHostVerification: !c.Bool(FlagTLSDisableHostVerification),
		}
	}

	session, err := gocql.NewSession(cassandraConfig, resolver.NewNoopResolver(), log.NewNoopLogger())
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
			namespaceID := gocql.UUIDToString(namespace["id"])
			fmt.Printf("namespaceId for namespace %v is %v \n", namespace, namespaceID)
		} else {
			namespace := res["namespace"].(map[string]interface{})
			namespaceID := gocql.UUIDToString(namespace["id"])
			fmt.Printf("namespaceId for namespace %v is %v \n", namespace, namespaceID)
		}
	}
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) {
	namespaceID := getRequiredOption(c, FlagNamespaceID)
	wid := getRequiredOption(c, FlagWorkflowID)
	numberOfShards := int32(c.Int(FlagNumberOfShards))

	if numberOfShards <= 0 {
		ErrorAndExit("numberOfShards is required", nil)
		return
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID, wid, numberOfShards)
	fmt.Printf("ShardId for namespace, workflowId: %v, %v is %v \n", namespaceID, wid, shardID)
}

// AdminDescribeTask outputs the details of a task given Task Id, Task Type, Shard Id and Visibility Timestamp
func AdminDescribeTask(c *cli.Context) {
	sid := int32(getRequiredIntOption(c, FlagShardID))
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
	executionManager, err := pFactory.NewExecutionManager()
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
	} else if category == enumsspb.TASK_CATEGORY_VISIBILITY {
		req := &persistence.GetVisibilityTaskRequest{ShardID: sid, TaskID: int64(tid)}
		task, err := executionManager.GetVisibilityTask(req)
		if err != nil {
			ErrorAndExit("Failed to get visibility task", err)
		}
		prettyPrintJSONObject(task)
	} else {
		ErrorAndExit("Failed to describe task", fmt.Errorf("Unrecognized task type, task_type=%v", category))
	}
}

// AdminListTasks outputs a list of a tasks for given Shard and Task Type
func AdminListTasks(c *cli.Context) {
	sid := int32(getRequiredIntOption(c, FlagShardID))
	categoryInt, err := stringToEnum(c.String(FlagTaskType), enumsspb.TaskCategory_value)
	if err != nil {
		ErrorAndExit("Failed to parse Task Type", err)
	}
	category := enumsspb.TaskCategory(categoryInt)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		ErrorAndExit(fmt.Sprintf("Task type %s is currently not supported", category), nil)
	}

	pFactory := CreatePersistenceFactory(c)
	executionManager, err := pFactory.NewExecutionManager()
	if err != nil {
		ErrorAndExit("Failed to initialize execution manager", err)
	}

	if category == enumsspb.TASK_CATEGORY_TRANSFER {
		req := &persistence.GetTransferTasksRequest{ShardID: sid}

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
	} else if category == enumsspb.TASK_CATEGORY_VISIBILITY {
		req := &persistence.GetVisibilityTasksRequest{ShardID: sid}

		paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
			req.NextPageToken = paginationToken
			response, err := executionManager.GetVisibilityTasks(req)
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

		req := &persistence.GetTimerTasksRequest{
			ShardID:      sid,
			MinTimestamp: minVis,
			MaxTimestamp: maxVis,
		}
		paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
			req.NextPageToken = paginationToken
			response, err := executionManager.GetTimerTasks(req)
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
	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.GetShard(ctx, &adminservice.GetShardRequest{ShardId: int32(sid)})

	if err != nil {
		ErrorAndExit("Failed to initialize shard manager", err)
	}

	prettyPrintJSONObject(response.ShardInfo)
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

	namespace := c.GlobalString(FlagNamespace)
	workflowID := c.String(FlagWorkflowID)
	shardID := c.Int(FlagShardID)
	historyAddr := c.String(FlagHistoryAddress)
	printFully := c.Bool(FlagPrintFullyDetail)

	flagsCount := 0
	if c.IsSet(FlagShardID) {
		flagsCount++
	}
	if c.GlobalIsSet(FlagNamespace) && c.IsSet(FlagWorkflowID) {
		flagsCount++
	}
	if c.IsSet(FlagHistoryAddress) {
		flagsCount++
	}
	if flagsCount != 1 {
		ErrorAndExit("must provide one and only one: shard id or namespace & workflow id or host address", nil)
		return
	}

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.DescribeHistoryHostRequest{}
	if c.IsSet(FlagShardID) {
		req.ShardId = int32(shardID)
	} else if c.GlobalIsSet(FlagNamespace) && c.IsSet(FlagWorkflowID) {
		req.Namespace = namespace
		req.WorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: workflowID}
	} else if c.IsSet(FlagHistoryAddress) {
		req.HostAddress = historyAddr
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
