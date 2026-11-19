package historydlq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"golang.org/x/sync/errgroup"
)

const queueListTokenPrefix = "historydlq:queues:"

type queueListCursor struct {
	Version       int
	QueueType     int32
	IncludeCHASM  bool
	LastQueueName string
}

func (s *Store) ListQueues(
	ctx context.Context,
	req *historyservice.ListQueuesRequest,
) (*historyservice.ListQueuesResponse, error) {
	includeCHASM := req.IncludeChasm && persistence.QueueV2Type(req.QueueType) == persistence.QueueTypeHistoryDLQ
	var lastQueueName string
	if bytes.HasPrefix(req.NextPageToken, []byte(queueListTokenPrefix)) || (includeCHASM && len(req.NextPageToken) > 0) {
		cursor, err := decodeQueueListCursor(req.NextPageToken, req.QueueType, includeCHASM)
		if err != nil {
			return nil, err
		}
		lastQueueName = cursor.LastQueueName
	}
	if !includeCHASM {
		return s.listQueueV2Page(ctx, req)
	}
	if req.PageSize <= 0 {
		return nil, consts.ErrInvalidPageSize
	}
	queues, hasMore, err := s.combinedQueuePage(ctx, lastQueueName, int(req.PageSize))
	if err != nil {
		return nil, err
	}
	response := &historyservice.ListQueuesResponse{Queues: queues}
	if hasMore {
		body, _ := json.Marshal(queueListCursor{
			Version:       1,
			QueueType:     req.QueueType,
			IncludeCHASM:  true,
			LastQueueName: queues[len(queues)-1].QueueName,
		})
		response.NextPageToken = append([]byte(queueListTokenPrefix), body...)
	}
	return response, nil
}

func decodeQueueListCursor(
	token []byte,
	queueType int32,
	includeCHASM bool,
) (queueListCursor, error) {
	invalid := func() (queueListCursor, error) { return queueListCursor{}, consts.ErrInvalidPaginationToken }
	if !bytes.HasPrefix(token, []byte(queueListTokenPrefix)) {
		return invalid()
	}
	var cursor queueListCursor
	decoder := json.NewDecoder(bytes.NewReader(token[len(queueListTokenPrefix):]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cursor) != nil || decoder.Decode(new(any)) != io.EOF || cursor.Version != 1 ||
		cursor.QueueType != queueType || !cursor.IncludeCHASM || !includeCHASM || cursor.LastQueueName == "" {
		return invalid()
	}
	return cursor, nil
}

func (s *Store) listQueueV2Page(
	ctx context.Context,
	req *historyservice.ListQueuesRequest,
) (*historyservice.ListQueuesResponse, error) {
	page, err := s.queueV2Manager.ListQueues(ctx, &persistence.ListQueuesRequest{
		QueueType:     persistence.QueueV2Type(req.QueueType),
		PageSize:      int(req.PageSize),
		NextPageToken: req.NextPageToken,
	})
	if err != nil {
		switch {
		case errors.Is(err, persistence.ErrNonPositiveListQueuesPageSize):
			return nil, consts.ErrInvalidPageSize
		case errors.Is(err, persistence.ErrNegativeListQueuesOffset), errors.Is(err, persistence.ErrInvalidListQueuesNextPageToken):
			return nil, consts.ErrInvalidPaginationToken
		default:
			return nil, serviceerror.NewUnavailablef("ListQueues failed. Error: %v", err)
		}
	}
	response := &historyservice.ListQueuesResponse{NextPageToken: page.NextPageToken}
	for _, queue := range page.Queues {
		response.Queues = append(response.Queues, &historyservice.ListQueuesResponse_QueueInfo{
			QueueName:     queue.QueueName,
			MessageCount:  queue.MessageCount,
			LastMessageId: queue.LastMessageID,
		})
	}
	return response, nil
}

func (s *Store) queueV2Inventory(ctx context.Context) (map[string]*historyservice.ListQueuesResponse_QueueInfo, error) {
	byName := make(map[string]*historyservice.ListQueuesResponse_QueueInfo)
	req := &historyservice.ListQueuesRequest{
		QueueType: int32(persistence.QueueTypeHistoryDLQ),
		PageSize:  1000,
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := s.listQueueV2Page(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, queue := range page.Queues {
			byName[queue.QueueName] = queue
		}
		if len(page.NextPageToken) == 0 {
			break
		}
		req.NextPageToken = page.NextPageToken
	}
	return byName, nil
}

func (s *Store) combinedQueuePage(
	ctx context.Context,
	lastQueueName string,
	pageSize int,
) (page []*historyservice.ListQueuesResponse_QueueInfo, hasMore bool, err error) {
	byName, err := s.queueV2Inventory(ctx)
	if err != nil {
		return nil, false, err
	}
	segments, err := dlq.DiscoverAllSegments(ctx, s.visibilityManager, s.clusterMetadata.GetCurrentClusterName())
	if err != nil {
		return nil, false, err
	}
	segmentQueueNames := make([]string, len(segments))
	for i, segment := range segments {
		name := persistence.GetHistoryTaskQueueName(
			int(segment.Queue.CategoryID), segment.Queue.SourceCluster, segment.Queue.TargetCluster,
		)
		segmentQueueNames[i] = name
		if _, ok := byName[name]; !ok {
			byName[name] = &historyservice.ListQueuesResponse_QueueInfo{QueueName: name, LastMessageId: -1}
		}
	}
	for name, queue := range byName {
		if name > lastQueueName {
			page = append(page, queue)
		}
	}
	slices.SortFunc(page, func(a, b *historyservice.ListQueuesResponse_QueueInfo) int {
		return strings.Compare(a.QueueName, b.QueueName)
	})
	hasMore = len(page) > pageSize
	if hasMore {
		page = page[:pageSize]
	}
	selectedQueues := make(map[string]*historyservice.ListQueuesResponse_QueueInfo, len(page))
	for _, queue := range page {
		selectedQueues[queue.QueueName] = queue
	}
	stats := make([]dlq.SegmentStats, len(segments))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for i, segment := range segments {
		if _, ok := selectedQueues[segmentQueueNames[i]]; !ok {
			continue
		}
		group.Go(func() error {
			if err := groupCtx.Err(); err != nil {
				return err
			}
			var err error
			stats[i], err = s.chasmClient.DescribeSegment(groupCtx, segment)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, false, err
	}
	for i, name := range segmentQueueNames {
		queue, ok := selectedQueues[name]
		if !ok {
			continue
		}
		queue.MessageCount += stats[i].PendingCount
		if stats[i].PendingCount > 0 {
			queue.LastMessageId = max(queue.LastMessageId, stats[i].LastMessageID)
		}
	}
	return page, hasMore, nil
}

// ListQueuesForMetrics includes pending entries from both history DLQ stores.
func (s *Store) ListQueuesForMetrics(
	ctx context.Context,
	req *persistence.ListQueuesRequest,
) (*persistence.ListQueuesResponse, error) {
	page, err := s.ListQueues(ctx, &historyservice.ListQueuesRequest{
		QueueType:     int32(req.QueueType),
		PageSize:      int32(req.PageSize),
		NextPageToken: req.NextPageToken,
		IncludeChasm:  true,
	})
	if err != nil {
		return nil, err
	}
	response := &persistence.ListQueuesResponse{NextPageToken: page.NextPageToken}
	for _, queue := range page.Queues {
		response.Queues = append(response.Queues, persistence.QueueInfo{
			QueueName:     queue.QueueName,
			MessageCount:  queue.MessageCount,
			LastMessageID: queue.LastMessageId,
		})
	}
	return response, nil
}
