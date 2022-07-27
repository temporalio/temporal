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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/temporalio/tctl-kit/pkg/color"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
)

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
		return fmt.Errorf("unable to read History Branch: %s", err)
	}

	allEvents := &historypb.History{}
	totalSize := 0
	for idx, b := range resp.HistoryBatches {
		totalSize += len(b.Data)
		fmt.Printf("======== batch %v, blob len: %v ======\n", idx+1, len(b.Data))
		historyBatch, err := serializer.DeserializeEvents(b)
		if err != nil {
			return fmt.Errorf("unable to deserialize Events: %s", err)
		}
		allEvents.Events = append(allEvents.Events, historyBatch...)
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(historyBatch)
		if err != nil {
			return fmt.Errorf("unable to encode History Events: %s", err)
		}
		fmt.Println(string(data))
	}
	fmt.Printf("======== total batches %v, total blob len: %v ======\n", len(history), totalSize)

	if outputFileName != "" {
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(allEvents.Events)
		if err != nil {
			return fmt.Errorf("unable to serialize History data: %s", err)
		}
		if err := os.WriteFile(outputFileName, data, 0666); err != nil {
			return fmt.Errorf("unable to export History data file: %s", err)
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
		return nil, fmt.Errorf("unable to get Workflow Mutable State: %s", err)
	}
	return resp, nil
}

// AdminDeleteWorkflow delete a workflow execution from Cassandra and visibility document from Elasticsearch.
func AdminDeleteWorkflow(c *cli.Context) error {
	return fmt.Errorf("not implemented")
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) error {
	namespaceID := c.String(FlagNamespaceID)
	wid := c.String(FlagWorkflowID)
	numberOfShards := int32(c.Int(FlagNumberOfShards))

	if numberOfShards <= 0 {
		return fmt.Errorf("missing required parameter number of Shards")
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
			return fmt.Errorf("unable to parse Task type: %s", err)
		}
		categoryValue = int32(categoryInt)
	}
	category := enumsspb.TaskCategory(categoryValue)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		return fmt.Errorf("missing required parameter Task type: %s", err)
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
		return fmt.Errorf("unable to list History tasks: %s", err)
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
		return fmt.Errorf("unable to parse Task Type: %s", err)
	}
	category := enumsspb.TaskCategory(categoryInt)
	if category == enumsspb.TASK_CATEGORY_UNSPECIFIED {
		return fmt.Errorf("task type %s is currently not supported", category)
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
		return fmt.Errorf("unable to remove Task: %s", err)
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
		return fmt.Errorf("unable to initialize Shard Manager: %s", err)
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
		return fmt.Errorf("unable to close Shard Task: %s", err)
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
		return fmt.Errorf("unable to describe Cluster: %s", err)
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
		return fmt.Errorf("unable to parse Heartbeat time: %s", err)
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
		return fmt.Errorf("unable to list Cluster Members: %s", err)
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
		return fmt.Errorf("missing required parameter either Shard Id, Namespace, Workflow Id or Host address")
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
		return fmt.Errorf("unable to describe History host: %s", err)
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
		return fmt.Errorf("unable to refresh Workflow Task: %s", err)
	} else {
		fmt.Println("Refresh workflow task succeeded.")
	}
	return nil
}
