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

package tdbg

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/temporalio/tctl-kit/pkg/color"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
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
func AdminShowWorkflow(c *cli.Context) error {
	namespace, err := getRequiredGlobalOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	wid := c.String(FlagWorkflowID)
	rid := c.String(FlagRunID)
	startEventId := c.Int64(FlagMinEventID)
	endEventId := c.Int64(FlagMaxEventID)
	startEventVerion := int64(c.Int(FlagMinEventVersion))
	endEventVersion := int64(c.Int(FlagMaxEventVersion))
	outputFileName := c.String(FlagOutputFilename)

	client := cFactory.AdminClient(c)

	serializer := serialization.NewSerializer()
	var history []*commonpb.DataBlob

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := client.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		StartEventId:      startEventId,
		EndEventId:        endEventId,
		StartEventVersion: startEventVerion,
		EndEventVersion:   endEventVersion,
		MaximumPageSize:   100,
		NextPageToken:     nil,
	})
	if err != nil {
		return fmt.Errorf("ReadHistoryBranch err: %s", err)
	}

	allEvents := &historypb.History{}
	totalSize := 0
	for idx, b := range resp.HistoryBatches {
		totalSize += len(b.Data)
		fmt.Printf("======== batch %v, blob len: %v ======\n", idx+1, len(b.Data))
		historyBatchThrift, err := serializer.DeserializeEvents(b)
		if err != nil {
			return fmt.Errorf("DeserializeEvents err: %s", err)
		}
		historyBatch := historyBatchThrift
		allEvents.Events = append(allEvents.Events, historyBatch...)
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(historyBatch)
		if err != nil {
			return fmt.Errorf("EncodeHistoryEvents err: %s", err)
		}
		fmt.Println(string(data))
	}
	fmt.Printf("======== total batches %v, total blob len: %v ======\n", len(history), totalSize)

	if outputFileName != "" {
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(allEvents.Events)
		if err != nil {
			return fmt.Errorf("Failed to serialize history data: %s", err)
		}
		if err := os.WriteFile(outputFileName, data, 0666); err != nil {
			return fmt.Errorf("Failed to export history data file: %s", err)
		}
	}
	return nil
}

// AdminDescribeWorkflow describe a new workflow execution for admin
func AdminDescribeWorkflow(c *cli.Context) error {
	resp, err := describeMutableState(c)
	if err != nil {
		return err
	}

	if resp != nil {
		fmt.Println(color.Green(c, "Cache mutable state:"))
		if resp.GetCacheMutableState() != nil {
			prettyPrintJSONObject(resp.GetCacheMutableState())
		}
		fmt.Println(color.Green(c, "Database mutable state:"))
		prettyPrintJSONObject(resp.GetDatabaseMutableState())

		fmt.Println(color.Green(c, "Current branch token:"))
		versionHistories := resp.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
		// if VersionHistories is set, then all branch infos are stored in VersionHistories
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
		if err != nil {
			fmt.Println(color.Red(c, "Unable to get current version history:"), err)
		} else {
			currentBranchToken := persistencespb.HistoryBranch{}
			err := currentBranchToken.Unmarshal(currentVersionHistory.BranchToken)
			if err != nil {
				fmt.Println(color.Red(c, "Unable to unmarshal current branch token:"), err)
			} else {
				prettyPrintJSONObject(currentBranchToken)
			}
		}

		fmt.Printf("History service address: %s\n", resp.GetHistoryAddr())
		fmt.Printf("Shard Id: %s\n", resp.GetShardId())
	}
	return nil
}

func describeMutableState(c *cli.Context) (*adminservice.DescribeMutableStateResponse, error) {
	adminClient := cFactory.AdminClient(c)

	namespace, err := getRequiredGlobalOption(c, FlagNamespace)
	if err != nil {
		return nil, err
	}
	wid := c.String(FlagWorkflowID)
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
		return nil, fmt.Errorf("Get workflow mutableState failed: %s", err)
	}
	return resp, nil
}

// AdminDeleteWorkflow delete a workflow execution from Cassandra and visibility document from Elasticsearch.
func AdminDeleteWorkflow(c *cli.Context) error {
	resp, err := describeMutableState(c)
	if err != nil {
		return err
	}
	namespaceID := resp.GetDatabaseMutableState().GetExecutionInfo().GetNamespaceId()
	runID := resp.GetDatabaseMutableState().GetExecutionState().GetRunId()

	adminDeleteVisibilityDocument(c, namespaceID)

	session, err := connectToCassandra(c)
	if err != nil {
		return err
	}

	shardID := resp.GetShardId()
	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		return fmt.Errorf("Unable to strconv.Atoi(shardID): %s", err)
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
			return fmt.Errorf("Unable to HistoryBranchFromBlob: %s", err)
		}
		fmt.Println("Deleting history events for:")
		prettyPrintJSONObject(branchInfo)
		execStore := cassandra.NewExecutionStore(session, log.NewNoopLogger())
		execMgr := persistence.NewExecutionManager(
			execStore,
			serialization.NewSerializer(),
			log.NewNoopLogger(),
			dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit),
		)
		ctx, cancel := newContext(c)
		defer cancel()

		err = execMgr.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			BranchToken: branchToken,
			ShardID:     int32(shardIDInt),
		})
		if err != nil {
			if c.Bool(FlagSkipErrorMode) {
				fmt.Println("Unable to DeleteHistoryBranch:", err)
			} else {
				return fmt.Errorf("Unable to DeleteHistoryBranch: %s", err)
			}
		}
	}

	exeStore := cassandra.NewExecutionStore(session, log.NewNoopLogger())
	req := &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     int32(shardIDInt),
		NamespaceID: namespaceID,
		WorkflowID:  c.String(FlagWorkflowID),
		RunID:       runID,
	}

	ctx, cancel := newContext(c)
	defer cancel()
	err = exeStore.DeleteWorkflowExecution(ctx, req)
	if err != nil {
		if c.Bool(FlagSkipErrorMode) {
			fmt.Printf("Unable to DeleteWorkflowExecution for RunID=%s: %v\n", runID, err)
		} else {
			return fmt.Errorf("Unable to DeleteWorkflowExecution for RunID=%s: %s", runID, err)
		}
	} else {
		fmt.Printf("DeleteWorkflowExecution for RunID=%s executed successfully.\n", runID)
	}

	deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     int32(shardIDInt),
		NamespaceID: namespaceID,
		WorkflowID:  c.String(FlagWorkflowID),
		RunID:       runID,
	}

	ctx, cancel = newContext(c)
	defer cancel()
	err = exeStore.DeleteCurrentWorkflowExecution(ctx, deleteCurrentReq)
	if err != nil {
		if c.Bool(FlagSkipErrorMode) {
			fmt.Printf("Unable to DeleteCurrentWorkflowExecution for RunID=%s: %v\n", runID, err)
		} else {
			return fmt.Errorf("Unable to DeleteCurrentWorkflowExecution for RunID=%s: %s", runID, err)
		}
	} else {
		fmt.Printf("DeleteCurrentWorkflowExecution for RunID=%s executed successfully.\n", runID)
	}
	return nil
}

func adminDeleteVisibilityDocument(c *cli.Context, namespaceID string) error {
	if !c.IsSet(FlagElasticsearchIndex) {
		prompt("Elasticsearch index name is not specified. Continue without visibility document deletion?", c.Bool(FlagAutoConfirm))
	}

	indexName := c.String(FlagElasticsearchIndex)
	esClient, err := newESClient(c)
	if err != nil {
		return err
	}

	query := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery(searchattribute.NamespaceID, namespaceID),
			elastic.NewTermQuery(searchattribute.WorkflowID, c.String(FlagWorkflowID)))
	if c.IsSet(FlagRunID) {
		query = query.Filter(elastic.NewTermQuery(searchattribute.RunID, c.String(FlagRunID)))
	}

	searchParams := &esclient.SearchParameters{
		Index:    indexName,
		Query:    query,
		PageSize: 10000,
	}
	searchResult, err := esClient.Search(context.Background(), searchParams)
	if err != nil {
		if c.Bool(FlagSkipErrorMode) {
			fmt.Println("Unable to search for visibility documents from Elasticsearch:", err)
		} else {
			return fmt.Errorf("Unable to search for visibility documents from Elasticsearch: %s", err)
		}
	}
	fmt.Println("Found", len(searchResult.Hits.Hits), "visibility documents.")
	for _, searchHit := range searchResult.Hits.Hits {
		err := esClient.Delete(context.Background(), indexName, searchHit.Id, math.MaxInt64)
		if err != nil {
			if c.Bool(FlagSkipErrorMode) {
				fmt.Println("Unable to delete visibility document from Elasticsearch:", err)
			} else {
				return fmt.Errorf("Unable to delete visibility document from Elasticsearch: %s", err)
			}
		} else {
			fmt.Println("Visibility document", searchHit.Id, "deleted successfully.")
		}
	}
	return nil
}

func readOneRow(query gocql.Query) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	err := query.MapScan(result)
	return result, err
}

func connectToCassandra(c *cli.Context) (gocql.Session, error) {
	host := c.String(FlagDBAddress)
	port := c.Int(FlagDBPort)

	cassandraConfig := config.Cassandra{
		Hosts:    host,
		Port:     port,
		User:     c.String(FlagUsername),
		Password: c.String(FlagPassword),
		Keyspace: c.String(FlagKeyspace),
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
		return nil, fmt.Errorf("connect to Cassandra failed: %s", err)
	}
	return session, nil
}

func newESClient(c *cli.Context) (esclient.CLIClient, error) {
	esUrl := c.String(FlagElasticsearchURL)
	parsedESUrl, err := url.Parse(esUrl)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse URL: %s", err)
	}

	esConfig := &esclient.Config{
		URL:      *parsedESUrl,
		Username: c.String(FlagElasticsearchUsername),
		Password: c.String(FlagElasticsearchPassword),
	}

	if c.IsSet(FlagVersion) {
		esConfig.Version = c.String(FlagVersion)
	}

	client, err := esclient.NewCLIClient(esConfig, log.NewCLILogger())
	if err != nil {
		return nil, fmt.Errorf("Unable to create Elasticsearch client: %s", err)
	}

	return client, nil
}

// AdminGetNamespaceIDOrName map namespace
func AdminGetNamespaceIDOrName(c *cli.Context) error {
	namespaceID := c.String(FlagNamespaceID)
	namespace := c.String(FlagNamespace)
	if len(namespaceID) == 0 && len(namespace) == 0 {
		return fmt.Errorf("Need either namespace or namespaceId")
	}

	session, err := connectToCassandra(c)
	if err != nil {
		return err
	}

	if len(namespaceID) > 0 {
		tmpl := "select namespace from namespaces where id = ? "
		query := session.Query(tmpl, namespaceID)
		res, err := readOneRow(query)
		if err != nil {
			return fmt.Errorf("readOneRow: %s", err)
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
				return fmt.Errorf("readOneRow for v2: %s", err)
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
	return nil
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) error {
	namespaceID := c.String(FlagNamespaceID)
	wid := c.String(FlagWorkflowID)
	numberOfShards := int32(c.Int(FlagNumberOfShards))

	if numberOfShards <= 0 {
		return fmt.Errorf("numberOfShards is required")
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID, wid, numberOfShards)
	fmt.Printf("ShardId for namespace, workflowId: %v, %v is %v \n", namespaceID, wid, shardID)
	return nil
}

// AdminListShardTasks outputs a list of a tasks for given Shard and Task Category
func AdminListShardTasks(c *cli.Context) error {
	sid := int32(c.Int(FlagShardID))
	categoryStr := c.String(FlagTaskType)
	categoryValue, err := stringToEnum(categoryStr, enumsspb.TaskCategory_value)
	if err != nil {
		categoryInt, err := strconv.Atoi(categoryStr)
		if err != nil {
			return fmt.Errorf("Failed to parse task type: %s", err)
		}
		categoryValue = int32(categoryInt)
	}
	category := enumsspb.TaskCategory(categoryValue)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		return fmt.Errorf("Task type is unspecified: %s", err)
	}

	client := cFactory.AdminClient(c)
	pageSize := defaultPageSize
	if c.IsSet(FlagPageSize) {
		pageSize = c.Int(FlagPageSize)
	}

	minFireTime, err := parseTime(c.String(FlagMinVisibilityTimestamp), time.Time{}, time.Now().UTC())
	if err != nil {
		return err
	}
	maxFireTime, err := parseTime(c.String(FlagMaxVisibilityTimestamp), time.Time{}, time.Now().UTC())
	if err != nil {
		return err
	}
	req := &adminservice.ListHistoryTasksRequest{
		ShardId:  sid,
		Category: category,
		TaskRange: &history.TaskRange{
			InclusiveMinTaskKey: &history.TaskKey{
				FireTime: timestamp.TimePtr(minFireTime),
				TaskId:   c.Int64(FlagMinTaskID),
			},
			ExclusiveMaxTaskKey: &history.TaskKey{
				FireTime: timestamp.TimePtr(maxFireTime),
				TaskId:   c.Int64(FlagMaxTaskID),
			},
		},
		BatchSize: int32(pageSize),
	}

	ctx, cancel := newContext(c)
	defer cancel()
	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		req.NextPageToken = paginationToken
		response, err := client.ListHistoryTasks(ctx, req)
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
	if err := paginate(c, paginationFunc, pageSize); err != nil {
		return fmt.Errorf("Failed to list history tasks: %s", err)
	}
	return nil
}

// AdminRemoveTask describes history host
func AdminRemoveTask(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)
	shardID := c.Int(FlagShardID)
	taskID := c.Int64(FlagTaskID)
	categoryInt, err := stringToEnum(c.String(FlagTaskType), enumsspb.TaskCategory_value)
	if err != nil {
		return fmt.Errorf("Failed to parse Task Type: %s", err)
	}
	category := enumsspb.TaskCategory(categoryInt)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		return fmt.Errorf("Task type %s is currently not supported", category)
	}
	var visibilityTimestamp int64
	if category == enumsspb.TASK_CATEGORY_TIMER {
		visibilityTimestamp = c.Int64(FlagTaskVisibilityTimestamp)
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
		return fmt.Errorf("Remove task has failed: %s", err)
	}
	return nil
}

// AdminDescribeShard describes shard by shard id
func AdminDescribeShard(c *cli.Context) error {
	sid := c.Int(FlagShardID)
	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.GetShard(ctx, &adminservice.GetShardRequest{ShardId: int32(sid)})

	if err != nil {
		return fmt.Errorf("Failed to initialize shard manager: %s", err)
	}

	prettyPrintJSONObject(response.ShardInfo)
	return nil
}

// AdminShardManagement describes history host
func AdminShardManagement(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)
	sid := c.Int(FlagShardID)

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.CloseShardRequest{}
	req.ShardId = int32(sid)

	_, err := adminClient.CloseShard(ctx, req)
	if err != nil {
		return fmt.Errorf("Close shard task has failed: %s", err)
	}
	return nil
}

// AdminListGossipMembers outputs a list of gossip members
func AdminListGossipMembers(c *cli.Context) error {
	roleFlag := c.String(FlagClusterMembershipRole)

	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return fmt.Errorf("Operation DescribeCluster failed: %s", err)
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
	return nil
}

// AdminListClusterMembers outputs a list of cluster members
func AdminListClusterMembers(c *cli.Context) error {
	role, _ := stringToEnum(c.String(FlagClusterMembershipRole), enumsspb.ClusterMemberRole_value)
	// TODO: refactor this: parseTime shouldn't be used for duration.
	heartbeatFlag, err := parseTime(c.String(FlagFrom), time.Time{}, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("Unable to parse heartbeat time: %s", err)
	}
	heartbeat := time.Duration(heartbeatFlag.UnixNano())

	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.ListClusterMembersRequest{
		Role:                enumsspb.ClusterMemberRole(role),
		LastHeartbeatWithin: &heartbeat,
	}

	resp, err := adminClient.ListClusterMembers(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to list cluster members: %s", err)
	}

	members := resp.ActiveMembers

	prettyPrintJSONObject(members)
	return nil
}

// AdminDescribeHistoryHost describes history host
func AdminDescribeHistoryHost(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)

	namespace := c.String(FlagNamespace)
	workflowID := c.String(FlagWorkflowID)
	shardID := c.Int(FlagShardID)
	historyAddr := c.String(FlagHistoryAddress)
	printFully := c.Bool(FlagPrintFullyDetail)

	flagsCount := 0
	if c.IsSet(FlagShardID) {
		flagsCount++
	}
	if c.IsSet(FlagNamespace) && c.IsSet(FlagWorkflowID) {
		flagsCount++
	}
	if c.IsSet(FlagHistoryAddress) {
		flagsCount++
	}
	if flagsCount != 1 {
		return fmt.Errorf("must provide one and only one: shard id or namespace & workflow id or host address")
	}

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.DescribeHistoryHostRequest{}
	if c.IsSet(FlagShardID) {
		req.ShardId = int32(shardID)
	} else if c.IsSet(FlagNamespace) && c.IsSet(FlagWorkflowID) {
		req.Namespace = namespace
		req.WorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: workflowID}
	} else if c.IsSet(FlagHistoryAddress) {
		req.HostAddress = historyAddr
	}

	resp, err := adminClient.DescribeHistoryHost(ctx, req)
	if err != nil {
		return fmt.Errorf("Describe history host failed: %s", err)
	}

	if !printFully {
		resp.ShardIds = nil
	}
	prettyPrintJSONObject(resp)
	return nil
}

// AdminRefreshWorkflowTasks refreshes all the tasks of a workflow
func AdminRefreshWorkflowTasks(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)

	namespace, err := getRequiredGlobalOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	wid := c.String(FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	_, err = adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		return fmt.Errorf("Refresh workflow task failed: %s", err)
	} else {
		fmt.Println("Refresh workflow task succeeded.")
	}
	return nil
}
