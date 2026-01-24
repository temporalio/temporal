package tdbg

import (
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
	pageSize := c.Int(FlagPageSize)
	workflowID := c.String(FlagWorkflowID)
	runID := c.String(FlagRunID)
	subqueue := c.Int(FlagSubqueue)
	var minPass int64
	if c.Bool(FlagFair) {
		minPass = c.Int64(FlagMinPass)
	} else if c.IsSet(FlagMinPass) {
		return fmt.Errorf("flag --%s is only valid with --%s", FlagMinPass, FlagFair)
	}
	client := clientFactory.AdminClient(c)

	req := adminservice.GetTaskQueueTasksRequest_builder{
		Namespace:     namespace,
		TaskQueue:     tqName,
		TaskQueueType: tqType,
		MinTaskId:     minTaskID,
		MaxTaskId:     maxTaskID,
		BatchSize:     int32(pageSize),
		Subqueue:      int32(subqueue),
		MinPass:       minPass,
	}.Build()

	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		ctx, cancel := newContext(c)
		defer cancel()

		req.SetNextPageToken(paginationToken)
		response, err := client.GetTaskQueueTasks(ctx, req)
		if err != nil {
			return nil, nil, err
		}

		tasks := response.GetTasks()
		if workflowID != "" {
			filteredTasks := tasks[:0]

			for _, task := range tasks {
				if task.GetData().GetWorkflowId() != workflowID {
					continue
				}
				if runID != "" && task.GetData().GetRunId() != runID {
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
		return items, response.GetNextPageToken(), nil
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

	tqPartition := taskqueuespb.TaskQueuePartition_builder{
		TaskQueue:     tqName,
		TaskQueueType: tqType,
	}.Build()
	if stickyName != "" {
		tqPartition.SetStickyName(stickyName)
	} else {
		tqPartition.SetNormalPartitionId(int32(partitionID))
	}

	client := clientFactory.AdminClient(c)
	req := adminservice.DescribeTaskQueuePartitionRequest_builder{
		Namespace:          namespace,
		TaskQueuePartition: tqPartition,
		BuildIds: taskqueuepb.TaskQueueVersionSelection_builder{
			BuildIds:    buildIDs,
			Unversioned: unversioned,
			AllActive:   allActive,
		}.Build(),
	}.Build()

	ctx, cancel := newContext(c)
	defer cancel()
	if response, e := client.DescribeTaskQueuePartition(ctx, req); e != nil {
		return fmt.Errorf("unable to describe Task Queue Partition: %w", e)
	} else {
		prettyPrintJSONObject(c, response)

	}
	return nil
}

// AdminForceUnloadTaskQueuePartition forcefully unloads a task queue partition
func AdminForceUnloadTaskQueuePartition(c *cli.Context, clientFactory ClientFactory) error {
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

	tqPartition := taskqueuespb.TaskQueuePartition_builder{
		TaskQueue:     tqName,
		TaskQueueType: tqType,
	}.Build()
	if stickyName != "" {
		tqPartition.SetStickyName(stickyName)
	} else {
		tqPartition.SetNormalPartitionId(int32(partitionID))
	}

	client := clientFactory.AdminClient(c)
	req := adminservice.ForceUnloadTaskQueuePartitionRequest_builder{
		Namespace:          namespace,
		TaskQueuePartition: tqPartition,
	}.Build()

	ctx, cancel := newContext(c)
	defer cancel()
	if response, e := client.ForceUnloadTaskQueuePartition(ctx, req); e != nil {
		return fmt.Errorf("unable to describe Task Queue Partition: %w", e)
	} else {
		prettyPrintJSONObject(c, response)

	}
	return nil
}
