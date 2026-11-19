package historydlq

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/api/serviceerror"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
)

// DiscoverSegments returns visibility candidates; absence is not proof of an empty queue.
func DiscoverSegments(ctx context.Context, visibilityManager chasm.VisibilityManager, queue QueueKey, inclusiveMaxMessageID int64) ([]SegmentRef, error) {
	if err := validateQueue(queue); err != nil {
		return nil, err
	}
	if inclusiveMaxMessageID < 0 {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a nonnegative message bound")
	}
	quote := func(value string) string { return sqlparser.String(sqlparser.NewStrVal([]byte(value))) }
	query := fmt.Sprintf("%s = %d AND %s = %s",
		categorySearchAttribute.Value(0).Alias, queue.CategoryID,
		sourceSearchAttribute.Value("").Alias, quote(queue.SourceCluster),
	)
	query += fmt.Sprintf(" AND %s >= 0 AND %s <= %d",
		minMessageIDSearchAttribute.Value(0).Alias,
		minMessageIDSearchAttribute.Value(0).Alias, inclusiveMaxMessageID,
	)
	return discoverSegments(ctx, visibilityManager, query, &queue, queue.TargetCluster)
}

// DiscoverAllSegments returns pending segment candidates across history DLQs.
func DiscoverAllSegments(ctx context.Context, visibilityManager chasm.VisibilityManager, currentClusterName string) ([]SegmentRef, error) {
	if currentClusterName == "" {
		return nil, serviceerror.NewInvalidArgument("history DLQ discovery requires the current cluster name")
	}
	query := minMessageIDSearchAttribute.Value(0).Alias + " >= 0"
	return discoverSegments(ctx, visibilityManager, query, nil, currentClusterName)
}

func discoverSegments(ctx context.Context, visibilityManager chasm.VisibilityManager, query string, queue *QueueKey, currentClusterName string) ([]SegmentRef, error) {
	request := &chasm.ListExecutionsRequest{NamespaceName: primitives.SystemLocalNamespace, Query: query, PageSize: 1000}
	found := make(map[SegmentRef]struct{})
	tokens := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := visibilityManager.ListExecutions(ctx, reflect.TypeFor[*Segment](), request)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, serviceerror.NewDataLoss("history DLQ visibility returned no response")
		}
		for _, execution := range response.GetExecutions() {
			ref, err := discoveredSegmentRef(execution, queue, currentClusterName)
			if err != nil {
				return nil, err
			}
			found[ref] = struct{}{}
		}
		if len(response.GetNextPageToken()) == 0 {
			break
		}
		token := string(response.GetNextPageToken())
		if _, ok := tokens[token]; ok {
			return nil, serviceerror.NewDataLoss("history DLQ visibility repeated a page token")
		}
		tokens[token] = struct{}{}
		request.NextPageToken = append([]byte(nil), response.GetNextPageToken()...)
	}
	var refs []SegmentRef
	for ref := range found {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b SegmentRef) int {
		if order := cmp.Compare(a.BusinessID, b.BusinessID); order != 0 {
			return order
		}
		return cmp.Compare(a.RunID, b.RunID)
	})
	return refs, nil
}

func discoveredSegmentRef(execution *chasmspb.VisibilityExecutionInfo, queue *QueueKey, currentClusterName string) (SegmentRef, error) {
	id := execution.GetBusinessId()
	suffix, ok := strings.CutPrefix(id, "historydlq:segment:v0:")
	decoded, err := hex.DecodeString(suffix)
	if !ok || err != nil || len(decoded) != 32 || execution.GetRunId() == "" {
		return SegmentRef{}, serviceerror.NewDataLoss("history DLQ visibility returned an invalid segment identity")
	}
	if queue != nil {
		return SegmentRef{Queue: *queue, BusinessID: id, RunID: execution.GetRunId()}, nil
	}
	fields := execution.GetChasmSearchAttributes().GetIndexedFields()
	var category int64
	discoveredQueue := QueueKey{TargetCluster: currentClusterName}
	if err := payload.Decode(fields[categorySearchAttribute.Value(0).Alias], &category); err != nil || category <= 0 || category > math.MaxInt32 {
		return SegmentRef{}, serviceerror.NewDataLoss("history DLQ visibility returned an invalid queue category")
	}
	discoveredQueue.CategoryID = int32(category)
	if err := payload.Decode(fields[sourceSearchAttribute.Value("").Alias], &discoveredQueue.SourceCluster); err != nil {
		return SegmentRef{}, serviceerror.NewDataLoss("history DLQ visibility returned an invalid source cluster")
	}
	if err := validateQueue(discoveredQueue); err != nil {
		return SegmentRef{}, serviceerror.NewDataLoss("history DLQ visibility returned an invalid queue identity")
	}
	return SegmentRef{Queue: discoveredQueue, BusinessID: id, RunID: execution.GetRunId()}, nil
}
