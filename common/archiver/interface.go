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

package archiver

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

type (
	// ArchiveHistoryRequest is request to Archive workflow history
	ArchiveHistoryRequest struct {
		ShardID              int
		NamespaceID          string
		Namespace            string
		WorkflowID           string
		RunID                string
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
	}

	// GetHistoryRequest is the request to Get archived history
	GetHistoryRequest struct {
		NamespaceID          string
		WorkflowID           string
		RunID                string
		CloseFailoverVersion *int64
		NextPageToken        []byte
		PageSize             int
	}

	// GetHistoryResponse is the response of Get archived history
	GetHistoryResponse struct {
		HistoryBatches []*historypb.History
		NextPageToken  []byte
	}

	// HistoryBootstrapContainer contains components needed by all history Archiver implementations
	HistoryBootstrapContainer struct {
		HistoryV2Manager persistence.HistoryManager
		Logger           log.Logger
		MetricsClient    metrics.Client
		ClusterMetadata  cluster.Metadata
		NamespaceCache   cache.NamespaceCache
	}

	// HistoryArchiver is used to archive history and read archived history
	HistoryArchiver interface {
		Archive(context.Context, URI, *ArchiveHistoryRequest, ...ArchiveOption) error
		Get(context.Context, URI, *GetHistoryRequest) (*GetHistoryResponse, error)
		ValidateURI(URI) error
	}

	// VisibilityBootstrapContainer contains components needed by all visibility Archiver implementations
	VisibilityBootstrapContainer struct {
		Logger          log.Logger
		MetricsClient   metrics.Client
		ClusterMetadata cluster.Metadata
		NamespaceCache  cache.NamespaceCache
	}

	// QueryVisibilityRequest is the request to query archived visibility records
	QueryVisibilityRequest struct {
		NamespaceID   string
		PageSize      int
		NextPageToken []byte
		Query         string
	}

	// QueryVisibilityResponse is the response of querying archived visibility records
	QueryVisibilityResponse struct {
		Executions    []*workflowpb.WorkflowExecutionInfo
		NextPageToken []byte
	}

	// VisibilityArchiver is used to archive visibility and read archived visibility
	VisibilityArchiver interface {
		Archive(context.Context, URI, *archiverspb.ArchiveVisibilityRequest, ...ArchiveOption) error
		Query(context.Context, URI, *QueryVisibilityRequest) (*QueryVisibilityResponse, error)
		ValidateURI(URI) error
	}
)
