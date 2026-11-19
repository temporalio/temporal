package historydlq

import (
	"context"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

type segmentClient interface {
	DescribeSegment(context.Context, dlq.SegmentRef) (dlq.SegmentStats, error)
	ReadSegmentMessages(context.Context, dlq.ReadSegmentMessagesRequest) (dlq.MessagePage, error)
}

type Store struct {
	queueV2Manager    persistence.HistoryTaskQueueManager
	categoryRegistry  tasks.TaskCategoryRegistry
	chasmClient       segmentClient
	visibilityManager chasm.VisibilityManager
	readCHASM         dynamicconfig.BoolPropertyFn
	clusterMetadata   cluster.Metadata
}

func NewStore(queueV2Manager persistence.HistoryTaskQueueManager, categoryRegistry tasks.TaskCategoryRegistry, chasmClient *dlq.Client, visibilityManager chasm.VisibilityManager, config *configs.Config, clusterMetadata cluster.Metadata) *Store {
	return &Store{queueV2Manager: queueV2Manager, categoryRegistry: categoryRegistry, chasmClient: chasmClient, visibilityManager: visibilityManager, readCHASM: config.TaskDLQReadCHASM, clusterMetadata: clusterMetadata}
}

func (s *Store) GetDLQTasks(ctx context.Context, req *historyservice.GetDLQTasksRequest) (*historyservice.GetDLQTasksResponse, error) {
	if req == nil || req.GetDlqKey() == nil {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a queue identity")
	}
	queue := dlq.QueueKey{CategoryID: req.DlqKey.TaskCategory, SourceCluster: req.DlqKey.SourceCluster, TargetCluster: req.DlqKey.TargetCluster}
	bound := int64(math.MaxInt64)
	if req.InclusiveMaxTaskMetadata != nil {
		bound = req.InclusiveMaxTaskMetadata.MessageId
	}
	if bound < 0 {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a nonnegative message bound")
	}
	// Preemptable DLQ activities use QueueV2 prefix deletion.
	readCHASM := s.readCHASM() && headers.GetCallerInfo(ctx).CallerType != headers.CallerTypePreemptable
	cursor, isQueueV2Token, err := decodeReadCursor(req.NextPageToken, queue, bound, readCHASM)
	if err != nil {
		return nil, err
	}
	category, err := api.GetTaskCategory(int(queue.CategoryID), s.categoryRegistry)
	if err != nil {
		return nil, err
	}
	queueKey := persistence.QueueKey{QueueType: persistence.QueueTypeHistoryDLQ, Category: category, SourceCluster: queue.SourceCluster, TargetCluster: queue.TargetCluster}
	if !readCHASM || isQueueV2Token {
		return s.readQueueV2Page(ctx, queueKey, req, bound)
	}
	if req.PageSize <= 0 {
		return nil, consts.ErrInvalidPageSize
	}
	if queue.SourceCluster == "" || queue.TargetCluster == "" {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a queue identity")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queueV2Tasks, err := s.readQueueV2Tasks(ctx, queueKey, bound)
	if err != nil {
		return nil, err
	}
	chasmMessages, err := s.readCHASMMessages(ctx, queue, bound)
	if err != nil {
		return nil, err
	}
	candidates := mergeReadCandidates(queueV2Tasks, chasmMessages)
	response := &historyservice.GetDLQTasksResponse{}
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cursor != nil && compareTuple(candidate.tuple, *cursor) <= 0 {
			continue
		}
		response.DlqTasks = append(response.DlqTasks, candidate.task)
		if len(response.DlqTasks) < int(req.PageSize) {
			continue
		}
		if i+1 < len(candidates) {
			response.NextPageToken = encodeReadCursor(queue, bound, candidate.tuple)
		}
		break
	}
	return response, nil
}
