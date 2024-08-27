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
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

// AdminListTaskQueueTasks displays task information
func AdminListTaskQueueTasks(c *cli.Context, clientFactory ClientFactory) error {
	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	tqName := c.String(FlagTaskQueue)
	tlTypeInt, err := StringToEnum(c.String(FlagTaskQueueType), enumspb.TaskQueueType_value)
	if err != nil {
		return fmt.Errorf("invalid task queue type: %v", err)
	}
	tqType := enumspb.TaskQueueType(tlTypeInt)
	if tqType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
		return fmt.Errorf("missing Task Queue type")
	}
	minTaskID := c.Int64(FlagMinTaskID)
	maxTaskID := c.Int64(FlagMaxTaskID)
	pageSize := defaultPageSize
	if c.IsSet(FlagPageSize) {
		pageSize = c.Int(FlagPageSize)
	}
	workflowID := c.String(FlagWorkflowID)
	runID := c.String(FlagRunID)

	client := clientFactory.AdminClient(c)

	req := &adminservice.GetTaskQueueTasksRequest{
		Namespace:     namespace,
		TaskQueue:     tqName,
		TaskQueueType: tqType,
		MinTaskId:     minTaskID,
		MaxTaskId:     maxTaskID,
		BatchSize:     int32(pageSize),
	}

	ctx, cancel := newContext(c)
	defer cancel()
	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		req.NextPageToken = paginationToken
		response, err := client.GetTaskQueueTasks(ctx, req)
		if err != nil {
			return nil, nil, err
		}

		tasks := response.Tasks
		if workflowID != "" {
			filteredTasks := tasks[:0]

			for _, task := range tasks {
				if task.Data.WorkflowId != workflowID {
					continue
				}
				if runID != "" && task.Data.RunId != runID {
					continue
				}
				filteredTasks = append(filteredTasks, task)
			}

			tasks = filteredTasks
		}

		var items []interface{}
		for _, task := range tasks {
			items = append(items, task)
		}
		return items, nil, nil
	}

	if err := paginate(c, paginationFunc, pageSize); err != nil {
		return fmt.Errorf("unable to list Task Queue Tasks: %v", err)
	}
	return nil
}

// AdminDescribeTaskQueuePartition displays task queue partition information
func AdminDescribeTaskQueuePartition(c *cli.Context, clientFactory ClientFactory) error {
	// extracting the namespace
	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	// extracting the task queue name
	tqName, err := getRequiredOption(c, FlagTaskQueue)
	if err != nil {
		return err
	}

	// extracting the task queue type
	tqTypeString, err := getRequiredOption(c, FlagTaskQueueType)
	if err != nil {
		return err
	}

	tlTypeInt, err := StringToEnum(tqTypeString, enumspb.TaskQueueType_value)
	if err != nil {
		return fmt.Errorf("invalid task queue type: %w", err)
	}
	tqType := enumspb.TaskQueueType(tlTypeInt)
	if tqType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
		return errors.New("invalid task queue type") // nolint
	}

	// extracting the task queue partition id
	partitionID := 0
	if c.IsSet(FlagPartitionID) {
		partitionID = c.Int(FlagPartitionID)
	}

	// extracting the task queue partition sticky name
	stickyName := ""
	if c.IsSet(FlagStickyName) {
		stickyName = c.String(FlagStickyName)
	}

	// extracting the task queue partition buildId's
	buildIDs := make([]string, 0)
	if c.IsSet(FlagBuildIDs) {
		buildIDs = c.StringSlice(FlagBuildIDs)
	}

	// extracting the unversioned flag
	unversioned := true
	if c.IsSet(FlagUnversioned) {
		unversioned = c.Bool(FlagUnversioned)
	}

	// extracting the allActive flag
	allActive := true
	if c.IsSet(FlagAllActive) {
		allActive = c.Bool(FlagAllActive)
	}

	tqPartition := &taskqueuespb.TaskQueuePartition{
		TaskQueue:     tqName,
		TaskQueueType: tqType,
	}
	if stickyName != "" {
		tqPartition.PartitionId = &taskqueuespb.TaskQueuePartition_StickyName{StickyName: stickyName}
	} else {
		tqPartition.PartitionId = &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(partitionID)}
	}

	client := clientFactory.AdminClient(c)
	req := &adminservice.DescribeTaskQueuePartitionRequest{
		Namespace:          namespace,
		TaskQueuePartition: tqPartition,
		BuildId: &taskqueue.TaskQueueVersionSelection{
			BuildIds:    buildIDs,
			Unversioned: unversioned,
			AllActive:   allActive,
		},
	}

	ctx, cancel := newContext(c)
	defer cancel()
	if response, e := client.DescribeTaskQueuePartition(ctx, req); e != nil {
		return fmt.Errorf("unable to describe Task Queue Partition: %w", e)
	} else {
		prettyPrintJSONObject(c, response)

	}
	return nil
}
