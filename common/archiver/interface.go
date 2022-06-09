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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package archiver

import (
	"context"

	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/common/searchattribute"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

type (
	// ArchiveHistoryRequest is request to Archive workflow history
	ArchiveHistoryRequest struct {
		ShardID              int32
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
		ExecutionManager persistence.ExecutionManager
		Logger           log.Logger
		MetricsClient    metrics.Client
		ClusterMetadata  cluster.Metadata
	}

	// HistoryArchiver is used to archive history and read archived history
	HistoryArchiver interface {
		// Archive is used to archive a Workflow's history. When the context expires the method should stop trying to archive.
		// Implementors are free to archive however they want, including implementing retries of sub-operations. The URI defines
		// the resource that histories should be archived into. The implementor gets to determine how to interpret the URI.
		// The Archive method may or may not be automatically retried by the caller. ArchiveOptions are used
		// to interact with these retries including giving the implementor the ability to cancel retries and record progress
		// between retry attempts.
		// This method will be invoked after a workflow passes its retention period.
		Archive(ctx context.Context, uri URI, request *ArchiveHistoryRequest, opts ...ArchiveOption) error
		// Get is used to access an archived history. When context expires this method should stop trying to fetch history.
		// The URI identifies the resource from which history should be accessed and it is up to the implementor to interpret this URI.
		// This method should emit api service errors - see the filestore as an example.
		Get(ctx context.Context, url URI, request *GetHistoryRequest) (*GetHistoryResponse, error)
		// ValidateURI is used to define what a valid URI for an implementation is.
		ValidateURI(uri URI) error
	}

	// VisibilityBootstrapContainer contains components needed by all visibility Archiver implementations
	VisibilityBootstrapContainer struct {
		Logger          log.Logger
		MetricsClient   metrics.Client
		ClusterMetadata cluster.Metadata
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
		// Archive is used to archive one Workflow visibility record.
		// Check the Archive method of the HistoryArchiver interface for parameters' meaning and requirements.
		// The only difference is that the ArchiveOption parameter won't include an option for recording process.
		// Please make sure your implementation is lossless. If any in-memory batching mechanism is used
		// then those batched records will be lost during server restarts. This method will be invoked when the Workflow closes.
		// Note that because of conflict resolution, it is possible for a Workflow to through the closing process multiple times,
		// which means that this method can be invoked more than once after a Workflow closes.
		Archive(ctx context.Context, uri URI, request *archiverspb.VisibilityRecord, opts ...ArchiveOption) error
		// Query is used to retrieve archived visibility records.
		// Check the Get() method of the HistoryArchiver interface in Step 2 for parameters' meaning and requirements.
		// The request includes a string field called query, which describes what kind of visibility records should be returned.
		// For example, it can be  some SQL-like syntax query string.
		// Your implementation is responsible for parsing and validating the query, and also returning all visibility records that match the query.
		// Currently the maximum context timeout passed into the method is 3 minutes, so it's acceptable if this method takes some time to run.
		Query(ctx context.Context, uri URI, request *QueryVisibilityRequest, saTypeMap searchattribute.NameTypeMap) (*QueryVisibilityResponse, error)
		// ValidateURI is used to define what a valid URI for an implementation is.
		ValidateURI(uri URI) error
	}
)
