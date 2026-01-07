package tdbg

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	historyImportBlobSize = 16
	historyImportPageSize = 256 * 1024 // 256K
)

// AdminShowWorkflow shows history
func AdminShowWorkflow(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	rid := c.String(FlagRunID)
	startEventId := c.Int64(FlagMinEventID)
	endEventId := c.Int64(FlagMaxEventID)
	startEventVerion := int64(c.Int(FlagMinEventVersion))
	endEventVersion := int64(c.Int(FlagMaxEventVersion))
	outputFileName := c.String(FlagOutputFilename)

	client := clientFactory.AdminClient(c)

	serializer := serialization.NewSerializer()

	ctx, cancel := newContext(c)
	defer cancel()

	nsID, err := getNamespaceID(c, clientFactory, namespace.Name(nsName))
	if err != nil {
		return err
	}

	var histories []*commonpb.DataBlob
	var token []byte
	for doContinue := true; doContinue; doContinue = len(token) != 0 {
		resp, err := client.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: nsID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: wid,
				RunId:      rid,
			},
			StartEventId:      startEventId,
			EndEventId:        endEventId,
			StartEventVersion: startEventVerion,
			EndEventVersion:   endEventVersion,
			MaximumPageSize:   100,
			NextPageToken:     token,
		})
		if err != nil {
			return fmt.Errorf("unable to recv History Branch: %s", err)
		}
		histories = append(histories, resp.HistoryBatches...)
		token = resp.NextPageToken
	}

	var historyBatches []*historypb.History
	totalSize := 0
	var errs []error
	for idx, b := range histories {
		totalSize += len(b.Data)
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintf(c.App.Writer, "======== batch %v, blob len: %v ======\n", idx+1, len(b.Data))
		historyBatch, err := serializer.DeserializeEvents(b)
		if err != nil {
			err := fmt.Errorf("unable to deserialize Events: %s", err)
			fmt.Fprintln(c.App.Writer, err)
			errs = append(errs, err)
			continue
		}
		historyBatches = append(historyBatches, &historypb.History{Events: historyBatch})
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistoryEvents(historyBatch)
		if err != nil {
			err := fmt.Errorf("unable to encode History Events: %s", err)
			// nolint:errcheck // assuming that write will succeed.
			fmt.Fprintln(c.App.Writer, err)
			text, terr := prototext.Marshal(&historypb.History{Events: historyBatch})
			if terr == nil {
				fmt.Fprintln(c.App.Writer, "marshal to text:")
				fmt.Fprintln(c.App.Writer, string(text))
			}
			errs = append(errs, err)
			continue
		}
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, string(data))
	}
	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "======== total batches %v, total blob len: %v ======\n", len(histories), totalSize)

	err = errors.Join(errs...)
	if err != nil {
		return err
	}

	if outputFileName != "" {
		encoder := codec.NewJSONPBEncoder()
		data, err := encoder.EncodeHistories(historyBatches)
		if err != nil {
			return fmt.Errorf("unable to serialize History data: %s", err)
		}
		if err := os.WriteFile(outputFileName, data, 0666); err != nil {
			return fmt.Errorf("unable to write History data file: %s", err)
		}
	}
	return nil
}

// AdminImportWorkflow imports history
func AdminImportWorkflow(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	rid, err := getRequiredOption(c, FlagRunID)
	if err != nil {
		return err
	}
	inputFileName := c.String(FlagInputFilename)

	client := clientFactory.AdminClient(c)

	serializer := serialization.NewSerializer()

	ctx, cancel := newContext(c)
	defer cancel()

	data, err := os.ReadFile(inputFileName)
	if err != nil {
		return fmt.Errorf("unable to read History data file: %s", err)
	}
	encoder := codec.NewJSONPBEncoder()
	historyBatches, err := encoder.DecodeHistories(data)
	if err != nil {
		return fmt.Errorf("unable to deserialize History data: %s", err)
	}

	versionHistory := &historyspb.VersionHistory{}
	for _, historyBatch := range historyBatches {
		for _, event := range historyBatch.Events {
			item := versionhistory.NewVersionHistoryItem(event.EventId, event.Version)
			if err := versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, item); err != nil {
				return fmt.Errorf("unable to generate version history: %s", err)
			}
		}
	}

	var token []byte
	var blobs []*commonpb.DataBlob
	blobSize := 0
	for i := 0; i < len(historyBatches)+1; i++ {
		if i < len(historyBatches) {
			historyBatch := historyBatches[i]
			blob, err := serializer.SerializeEvents(historyBatch.Events)
			if err != nil {
				return fmt.Errorf("unable to deserialize Events: %s", err)
			}
			blobSize += len(blob.Data)
			blobs = append(blobs, blob)
		}
		if blobSize >= historyImportBlobSize ||
			len(blobs) >= historyImportPageSize ||
			(i == len(historyBatches) && len(blobs) > 0) {
			resp, err := client.ImportWorkflowExecution(ctx, &adminservice.ImportWorkflowExecutionRequest{
				Namespace: nsName,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: wid,
					RunId:      rid,
				},
				HistoryBatches: blobs,
				VersionHistory: versionHistory,
				Token:          token,
			})
			if err != nil {
				return fmt.Errorf("unable to send History Branch: %s", err)
			}
			token = resp.Token

			blobs = []*commonpb.DataBlob{}
			blobSize = 0
		}
	}
	if len(blobs) != 0 {
		return errors.New("unable to import workflow events, some events are not sent")
	}
	// call with empty history to commit
	resp, err := client.ImportWorkflowExecution(ctx, &adminservice.ImportWorkflowExecutionRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		HistoryBatches: []*commonpb.DataBlob{},
		VersionHistory: versionHistory,
		Token:          token,
	})
	if err != nil {
		return fmt.Errorf("unable to import workflow events: %s", err)
	}
	if len(resp.Token) != 0 {
		return errors.New("unable to import workflow events, not committed")
	}
	return nil
}

// AdminDescribeExecution describes a Temporal execution (CHASM tree or workflow).
func AdminDescribeExecution(c *cli.Context, clientFactory ClientFactory) error {
	resp, err := describeMutableState(c, clientFactory)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("no mutable state returned")
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintln(c.App.Writer, color.GreenString("Cache mutable state:"))
	if resp.GetCacheMutableState() != nil {
		prettyPrintJSONObject(c, resp.GetCacheMutableState())
	}
	if resp.GetDatabaseMutableState() != nil {
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, color.GreenString("Database mutable state:"))
		prettyPrintJSONObject(c, resp.GetDatabaseMutableState())
	}

	// CHASM executions also print their tree.
	if len(resp.GetDatabaseMutableState().GetChasmNodes()) > 0 {
		err := dumpChasmTree(resp, c)
		if err != nil {
			// nolint:errcheck // assuming that write will succeed.
			fmt.Fprintln(c.App.Writer, color.RedString("Unable to dump CHASM tree:"), err)
		}
	}

	if resp.GetDatabaseMutableState() != nil {
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, color.GreenString("Current branch token:"))
		versionHistories := resp.GetDatabaseMutableState().GetExecutionInfo().GetVersionHistories()
		// if VersionHistories is set, then all branch infos are stored in VersionHistories
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
		if err != nil {
			// nolint:errcheck // assuming that write will succeed.
			fmt.Fprintln(c.App.Writer, color.RedString("Unable to get current version history:"), err)
		} else {
			currentBranchToken := persistencespb.HistoryBranch{}
			err := currentBranchToken.Unmarshal(currentVersionHistory.BranchToken)
			if err != nil {
				// nolint:errcheck // assuming that write will succeed.
				fmt.Fprintln(c.App.Writer, color.RedString("Unable to unmarshal current branch token:"), err)
			} else {
				prettyPrintJSONObject(c, &currentBranchToken)
			}
		}
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "History service address: %s\n", resp.GetHistoryAddr())
	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "Shard Id: %s\n", resp.GetShardId())

	return nil
}

func dumpChasmTree(resp *adminservice.DescribeMutableStateResponse, c *cli.Context) error {
	chasmNodes := resp.GetDatabaseMutableState().GetChasmNodes()
	if len(chasmNodes) == 0 {
		return nil
	}

	logger := log.NewNoopLogger()
	registry, err := newChasmRegistry(logger)
	if err != nil {
		return fmt.Errorf("failed to create CHASM registry: %w", err)
	}

	decodedNodes, err := decodeChasmNodes(chasmNodes, registry)
	if err != nil {
		return fmt.Errorf("failed to decode CHASM nodes: %w", err)
	}

	fmt.Fprintln(c.App.Writer, color.GreenString("CHASM Tree Nodes:")) // nolint:errcheck // assuming that write will succeed.
	prettyPrintJSONObject(c, decodedNodes)

	return nil
}

func describeMutableState(c *cli.Context, clientFactory ClientFactory) (*adminservice.DescribeMutableStateResponse, error) {
	adminClient := clientFactory.AdminClient(c)

	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return nil, err
	}
	bid, err := getRequiredOption(c, FlagBusinessID)
	if err != nil {
		return nil, err
	}
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: bid,
			RunId:      rid,
		},
		Archetype: getArchetypeWithDefault(c, chasm.WorkflowArchetype),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get Mutable State: %s", err)
	}
	return resp, nil
}

// AdminDeleteWorkflow force deletes a workflow's mutable state (both concrete and current), history, and visibility
// records as long as it's possible.
// It should only be used as a troubleshooting tool since no additional check will be done before the deletion.
// (e.g. if a child workflow has recorded its result in the parent workflow)
// Please use normal workflow delete command to gracefully delete a workflow execution.
func AdminDeleteWorkflow(c *cli.Context, clientFactory ClientFactory, prompter *Prompter) error {
	adminClient := clientFactory.AdminClient(c)

	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	rid := c.String(FlagRunID)

	msg := fmt.Sprintf("Namespace: %s WorkflowID: %s RunID: %s\nForce delete above workflow execution?", namespace, wid, rid)
	prompter.Prompt(msg)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		Archetype: getArchetypeWithDefault(c, chasm.WorkflowArchetype),
	})
	if err != nil {
		return fmt.Errorf("unable to delete workflow execution: %s", err)
	}

	if len(resp.Warnings) != 0 {
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, "Warnings:")
		for _, warning := range resp.Warnings {
			fmt.Fprintf(c.App.Writer, "- %s\n", warning)
		}
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, "")
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintln(c.App.Writer, "Workflow execution deleted.")

	return nil
}

// AdminGetShardID get shardID
func AdminGetShardID(c *cli.Context) error {
	namespaceID := c.String(FlagNamespaceID)
	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	numberOfShards := int32(c.Int(FlagNumberOfShards))

	if numberOfShards <= 0 {
		return fmt.Errorf("missing required parameter number of Shards")
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID, wid, numberOfShards)
	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "ShardId for namespace, workflowId: %v, %v is %v \n", namespaceID, wid, shardID)
	return nil
}

// getCategory first searches the registry for the category by the [tasks.Category.Name].
func getCategory(registry tasks.TaskCategoryRegistry, key string) (tasks.Category, error) {
	for _, category := range registry.GetCategories() {
		if strings.EqualFold(category.Name(), key) {
			return category, nil
		}
	}
	return tasks.Category{}, fmt.Errorf("unknown task category %q", key)
}

// AdminListShardTasks outputs a list of a tasks for given Shard and Task Category
func AdminListShardTasks(c *cli.Context, clientFactory ClientFactory, registry tasks.TaskCategoryRegistry) error {
	sid := int32(c.Int(FlagShardID))
	categoryStr := c.String(FlagTaskCategory)
	category, err := getCategory(registry, categoryStr)
	if err != nil {
		return err
	}

	client := clientFactory.AdminClient(c)
	pageSize := defaultPageSize
	if c.IsSet(FlagPageSize) {
		pageSize = c.Int(FlagPageSize)
	}

	minFireTime, err := parseTime(c.String(FlagMinVisibilityTimestamp), time.Unix(0, 0), time.Now().UTC())
	if err != nil {
		return err
	}
	maxFireTime, err := parseTime(c.String(FlagMaxVisibilityTimestamp), time.Unix(0, 0), time.Now().UTC())
	if err != nil {
		return err
	}
	req := &adminservice.ListHistoryTasksRequest{
		ShardId:  sid,
		Category: int32(category.ID()),
		TaskRange: &historyspb.TaskRange{
			InclusiveMinTaskKey: &historyspb.TaskKey{
				FireTime: timestamppb.New(minFireTime),
				TaskId:   c.Int64(FlagMinTaskID),
			},
			ExclusiveMaxTaskKey: &historyspb.TaskKey{
				FireTime: timestamppb.New(maxFireTime),
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
func AdminRemoveTask(
	c *cli.Context,
	clientFactory ClientFactory,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
) error {
	adminClient := clientFactory.AdminClient(c)
	shardID := c.Int(FlagShardID)
	taskID := c.Int64(FlagTaskID)
	category, err := getCategory(taskCategoryRegistry, c.String(FlagTaskCategory))
	if err != nil {
		return err
	}
	var visibilityTimestamp int64
	if category.Type() == tasks.CategoryTypeScheduled {
		if !c.IsSet(FlagTaskVisibilityTimestamp) {
			//nolint:errorlint
			return fmt.Errorf("%s is required to remove %s tasks", FlagTaskVisibilityTimestamp, category.Name())
		}
		visibilityTimestamp = c.Int64(FlagTaskVisibilityTimestamp)
	}

	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.RemoveTaskRequest{
		ShardId:        int32(shardID),
		Category:       int32(category.ID()),
		TaskId:         taskID,
		VisibilityTime: timestamppb.New(timestamp.UnixOrZeroTime(visibilityTimestamp)),
	}

	_, err = adminClient.RemoveTask(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to remove Task: %s", err)
	}
	return nil
}

// AdminDescribeShard describes shard by shard id
func AdminDescribeShard(c *cli.Context, clientFactory ClientFactory) error {
	sid := c.Int(FlagShardID)
	adminClient := clientFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.GetShard(ctx, &adminservice.GetShardRequest{ShardId: int32(sid)})

	if err != nil {
		return fmt.Errorf("unable to initialize Shard Manager: %s", err)
	}

	prettyPrintJSONObject(c, response.ShardInfo)
	return nil
}

// AdminShardManagement describes history host
func AdminShardManagement(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)
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
func AdminListGossipMembers(c *cli.Context, clientFactory ClientFactory) error {
	roleFlag := c.String(FlagClusterMembershipRole)

	adminClient := clientFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return fmt.Errorf("unable to describe Cluster: %s", err)
	}

	members := response.MembershipInfo.Rings
	if roleFlag != string(primitives.AllServices) {
		all := members

		members = members[:0]
		for _, v := range all {
			if roleFlag == v.Role {
				members = append(members, v)
			}
		}
	}

	prettyPrintJSONObject(c, members)
	return nil
}

// AdminListClusterMembers outputs a list of cluster members
func AdminListClusterMembers(c *cli.Context, clientFactory ClientFactory) error {
	role, _ := StringToEnum(c.String(FlagClusterMembershipRole), enumsspb.ClusterMemberRole_value)
	// TODO: refactor this: parseTime shouldn't be used for duration.
	heartbeatFlag, err := parseTime(c.String(FlagFrom), time.Time{}, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("unable to parse Heartbeat time: %s", err)
	}
	heartbeat := time.Duration(heartbeatFlag.UnixNano())

	adminClient := clientFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	req := &adminservice.ListClusterMembersRequest{
		Role:                enumsspb.ClusterMemberRole(role),
		LastHeartbeatWithin: durationpb.New(heartbeat),
	}

	resp, err := adminClient.ListClusterMembers(ctx, req)
	if err != nil {
		return fmt.Errorf("unable to list Cluster Members: %s", err)
	}

	members := resp.ActiveMembers

	prettyPrintJSONObject(c, members)
	return nil
}

// AdminDescribeHistoryHost describes history host
func AdminDescribeHistoryHost(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)

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
	prettyPrintJSONObject(c, resp)
	return nil
}

func adminRefreshWorkflowTasks(c *cli.Context, clientFactory ClientFactory, prompter *Prompter) error {
	if c.IsSet(FlagVisibilityQuery) && c.IsSet(FlagWorkflowID) && c.IsSet(FlagRunID) {
		return errors.New("setting parameter visibility query with workflow ID and run ID is not allowed")
	}
	if c.IsSet(FlagVisibilityQuery) && !c.IsSet(FlagWorkflowID) && !c.IsSet(FlagRunID) {
		return AdminBatchRefreshWorkflowTasks(c, clientFactory, prompter)
	}
	return AdminRefreshWorkflowTasks(c, clientFactory)
}

// AdminRefreshWorkflowTasks refreshes all the tasks of a workflow
func AdminRefreshWorkflowTasks(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	nsID, err := getNamespaceID(c, clientFactory, namespace.Name(nsName))
	if err != nil {
		return err
	}

	_, err = adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId: nsID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		Archetype: getArchetypeWithDefault(c, chasm.WorkflowArchetype),
	})
	if err != nil {
		return fmt.Errorf("unable to refresh Workflow Task: %s", err)
	} else {
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, "Refresh workflow task succeeded.")
	}
	return nil
}

// AdminBatchRefreshWorkflowTasks starts a batch job to refresh workflow tasks for multiple workflows
func AdminBatchRefreshWorkflowTasks(c *cli.Context, clientFactory ClientFactory, prompter *Prompter) error {
	adminClient := clientFactory.AdminClient(c)
	workflowClient := clientFactory.WorkflowClient(c)

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	query, err := getRequiredOption(c, FlagVisibilityQuery)
	if err != nil {
		return err
	}

	reason, err := getRequiredOption(c, FlagReason)
	if err != nil {
		return err
	}

	jobID := c.String(FlagJobID)
	if jobID == "" {
		jobID = fmt.Sprintf("batch-refresh-%d", time.Now().UnixNano())
	}

	ctx, cancel := newContext(c)
	defer cancel()

	// Count workflows matching the query to confirm with user
	countResp, err := workflowClient.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: nsName,
		Query:     query,
	})
	if err != nil {
		return fmt.Errorf("unable to count workflow executions: %w", err)
	}

	msg := fmt.Sprintf("Will refresh tasks for %d execution(s) matching query %q in namespace %q. Continue Y/N?",
		countResp.GetCount(), query, nsName)
	prompter.Prompt(msg)

	_, err = adminClient.StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:       nsName,
		VisibilityQuery: query,
		JobId:           jobID,
		Reason:          reason,
		Identity:        getCurrentUserFromEnv(),
		Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
			RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
		},
	})
	if err != nil {
		return fmt.Errorf("unable to start batch refresh workflow tasks: %w", err)
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "Batch Refresh Workflow Tasks started successfully for Job ID: %s\n", jobID)
	return nil
}

// AdminRebuildMutableState rebuild a workflow mutable state using persisted history events
func AdminRebuildMutableState(c *cli.Context, clientFactory ClientFactory) error {
	adminClient := clientFactory.AdminClient(c)

	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}
	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	_, err = adminClient.RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
	})
	if err != nil {
		return fmt.Errorf("rebuild mutable state failed: %s", err)
	} else {
		// nolint:errcheck // assuming that write will succeed.
		fmt.Fprintln(c.App.Writer, "rebuild mutable state succeeded.")
	}
	return nil
}

// AdminReplicateWorkflow force replicates a workflow by generating replication tasks
func AdminReplicateWorkflow(
	c *cli.Context,
	clientFactory ClientFactory,
) error {
	adminClient := clientFactory.AdminClient(c)

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return err
	}
	rid := c.String(FlagRunID)

	ctx, cancel := newContext(c)
	defer cancel()

	_, err = adminClient.GenerateLastHistoryReplicationTasks(ctx, &adminservice.GenerateLastHistoryReplicationTasksRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		Archetype: getArchetypeWithDefault(c, chasm.WorkflowArchetype),
	})
	if err != nil {
		return fmt.Errorf("unable to replicate workflow: %w", err)
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintln(c.App.Writer, "Replication tasks generated successfully.")
	return nil
}
