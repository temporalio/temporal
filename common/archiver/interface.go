// Copyright (c) 2017 Uber Technologies, Inc.
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
	"errors"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

var (
	// ErrArchiveNonRetriable is the error every Archiver implementation should return when
	// a non-retriable error is encountered.
	// If errors of other types are returned, method might be retried by caller
	ErrArchiveNonRetriable = errors.New("archive method encountered a non-retriable error")
)

type (
	// ArchiveHistoryRequest is request to Archive
	ArchiveHistoryRequest struct {
		ShardID              int
		DomainID             string
		DomainName           string
		WorkflowID           string
		RunID                string
		EventStoreVersion    int32
		BranchToken          []byte
		NextEventID          int64
		CloseFailoverVersion int64
	}

	// GetHistoryRequest is the request to Get archived history
	GetHistoryRequest struct {
		DomainID             string
		WorkflowID           string
		RunID                string
		CloseFailoverVersion *int64
		NextPageToken        []byte
		PageSize             int
	}

	// GetHistoryResponse is the response of Get archived history
	GetHistoryResponse struct {
		HistoryBatches []*shared.History
		NextPageToken  []byte
	}

	// HistoryBootstrapContainer contains components needed by all history Archiver implementations
	HistoryBootstrapContainer struct {
		HistoryManager   persistence.HistoryManager
		HistoryV2Manager persistence.HistoryV2Manager
		Logger           log.Logger
		MetricsClient    metrics.Client
	}

	// HistoryArchiver is used to archive history and read archived history
	HistoryArchiver interface {
		Archive(ctx context.Context, URI string, request *ArchiveHistoryRequest, opts ...ArchiveOption) error
		Get(ctx context.Context, URI string, request *GetHistoryRequest) (*GetHistoryResponse, error)
		ValidateURI(URI string) error
	}

	// ycyang TODO: implement visibility archiver

	// ArchiveVisibilityRequest is request to Archive
	ArchiveVisibilityRequest struct{}

	// GetVisibilityRequest is the request to Get archived visibility records
	GetVisibilityRequest struct{}

	// GetVisibilityResponse is the response of Get archived visibility records
	GetVisibilityResponse struct{}

	// VisibilityBootstrapContainer contains components needed by all visibility Archiver implementations
	VisibilityBootstrapContainer struct{}

	// VisibilityArchiver is used to archive visibility and read archived visibility
	VisibilityArchiver interface {
		Archive(ctx context.Context, URI string, request *ArchiveVisibilityRequest, opts ...ArchiveOption) error
		Get(ctx context.Context, URI string, request *GetVisibilityRequest) (*GetVisibilityResponse, error)
		ValidateURI(URI string) error
	}
)
