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

package persistence

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/versionhistory"
)

type (
	XDCCacheKey struct {
		WorkflowKey definition.WorkflowKey
		MinEventID  int64 // inclusive
		MaxEventID  int64 // exclusive
		Version     int64
	}
	XDCCacheValue struct {
		BaseWorkflowInfo    *workflowspb.BaseExecutionInfo
		VersionHistoryItems []*historyspb.VersionHistoryItem
		EventBlob           *commonpb.DataBlob
	}

	XDCCache interface {
		Put(key XDCCacheKey, value XDCCacheValue)
		Get(key XDCCacheKey) (XDCCacheValue, bool)
	}

	XDCCacheImpl struct {
		cache cache.Cache
	}
)

const (
	xdcMinCacheSize = 64 * 1024 // 64KB
)

var _ XDCCache = (*XDCCacheImpl)(nil)
var _ cache.SizeGetter = XDCCacheValue{}

func NewXDCCacheKey(
	workflowKey definition.WorkflowKey,
	minEventID int64,
	maxEventID int64,
	version int64,
) XDCCacheKey {
	return XDCCacheKey{
		WorkflowKey: workflowKey,
		MinEventID:  minEventID,
		MaxEventID:  maxEventID,
		Version:     version,
	}
}

func NewXDCCacheValue(
	baseWorkflowInfo *workflowspb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	eventBlob *commonpb.DataBlob,
) XDCCacheValue {
	return XDCCacheValue{
		BaseWorkflowInfo:    baseWorkflowInfo,
		VersionHistoryItems: versionHistoryItems,
		EventBlob:           eventBlob,
	}
}

func (v XDCCacheValue) CacheSize() int {
	size := 0
	for _, item := range v.VersionHistoryItems {
		size += item.Size()
	}
	return v.BaseWorkflowInfo.Size() + size + v.EventBlob.Size()
}

func NewEventsBlobCache(
	maxBytes int,
	ttl time.Duration,
) *XDCCacheImpl {
	return &XDCCacheImpl{
		cache: cache.New(
			max(xdcMinCacheSize, maxBytes),
			&cache.Options{
				TTL: ttl,
				Pin: false,
			},
			metrics.NoopMetricsHandler,
		),
	}
}

func (e *XDCCacheImpl) Put(
	key XDCCacheKey,
	value XDCCacheValue,
) {
	e.cache.Put(key, value)
}

func (e *XDCCacheImpl) Get(key XDCCacheKey) (XDCCacheValue, bool) {
	value := e.cache.Get(key)
	if value == nil {
		return XDCCacheValue{}, false
	}
	return value.(XDCCacheValue), true
}

func GetXDCCacheValue(
	executionInfo *persistencepb.WorkflowExecutionInfo,
	eventID int64,
	version int64,
) ([]*historyspb.VersionHistoryItem, []byte, *workflowspb.BaseExecutionInfo, error) {
	baseWorkflowInfo := CopyBaseWorkflowInfo(executionInfo.BaseExecutionInfo)
	versionHistories := executionInfo.VersionHistories
	versionHistoryIndex, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
		versionHistories,
		versionhistory.NewVersionHistoryItem(
			eventID,
			version,
		),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	versionHistoryBranch, err := versionhistory.GetVersionHistory(versionHistories, versionHistoryIndex)
	if err != nil {
		return nil, nil, nil, err
	}
	return versionhistory.CopyVersionHistory(versionHistoryBranch).GetItems(), versionHistoryBranch.GetBranchToken(), baseWorkflowInfo, nil
}

func CopyBaseWorkflowInfo(
	baseWorkflowInfo *workflowspb.BaseExecutionInfo,
) *workflowspb.BaseExecutionInfo {
	if baseWorkflowInfo == nil {
		return nil
	}
	return &workflowspb.BaseExecutionInfo{
		RunId:                            baseWorkflowInfo.RunId,
		LowestCommonAncestorEventId:      baseWorkflowInfo.LowestCommonAncestorEventId,
		LowestCommonAncestorEventVersion: baseWorkflowInfo.LowestCommonAncestorEventVersion,
	}
}
