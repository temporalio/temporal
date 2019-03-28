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

package frontend

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"strconv"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/admin/adminserviceserver"
	h "github.com/uber/cadence/.gen/go/history"
	hist "github.com/uber/cadence/.gen/go/history"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	historyService "github.com/uber/cadence/service/history"
)

var _ adminserviceserver.Interface = (*AdminHandler)(nil)

type (
	// AdminHandler - Thrift handler inteface for admin service
	AdminHandler struct {
		status                int32
		numberOfHistoryShards int
		service.Service
		history       history.Client
		domainCache   cache.DomainCache
		metricsClient metrics.Client
		historyMgr    persistence.HistoryManager
		historyV2Mgr  persistence.HistoryV2Manager
		startWG       sync.WaitGroup
	}
)

// NewAdminHandler creates a thrift handler for the cadence admin service
func NewAdminHandler(
	sVice service.Service, numberOfHistoryShards int, metadataMgr persistence.MetadataManager,
	historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager) *AdminHandler {
	handler := &AdminHandler{
		status:                common.DaemonStatusInitialized,
		numberOfHistoryShards: numberOfHistoryShards,
		Service:               sVice,
		domainCache:           cache.NewDomainCache(metadataMgr, sVice.GetClusterMetadata(), sVice.GetMetricsClient(), sVice.GetLogger()),
		historyMgr:            historyMgr,
		historyV2Mgr:          historyV2Mgr,
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
	return handler
}

// Start starts the handler
func (adh *AdminHandler) Start() error {
	if !atomic.CompareAndSwapInt32(&adh.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return nil
	}

	adh.domainCache.Start()
	adh.Service.GetDispatcher().Register(adminserviceserver.New(adh))
	adh.Service.Start()

	adh.history = adh.GetClientBean().GetHistoryClient()
	adh.metricsClient = adh.Service.GetMetricsClient()
	adh.startWG.Done()
	return nil
}

// Stop stops the handler
func (adh *AdminHandler) Stop() {
	if !atomic.CompareAndSwapInt32(&adh.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	adh.Service.Stop()
	adh.domainCache.Stop()
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (adh *AdminHandler) DescribeWorkflowExecution(ctx context.Context, request *admin.DescribeWorkflowExecutionRequest) (resp *admin.DescribeWorkflowExecutionResponse, retError error) {
	defer logging.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminDescribeWorkflowExecutionScope
	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	shardID := common.WorkflowIDToHistoryShard(*request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDstr := string(shardID)
	shardIDForOutput := strconv.Itoa(shardID)

	historyHost, err := adh.GetMembershipMonitor().Lookup(common.HistoryServiceName, shardIDstr)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	domainID, err := adh.domainCache.GetDomainID(request.GetDomain())

	historyAddr := historyHost.GetAddress()
	resp2, err := adh.history.DescribeMutableState(ctx, &hist.DescribeMutableStateRequest{
		DomainUUID: &domainID,
		Execution:  request.Execution,
	})
	if err != nil {
		return &admin.DescribeWorkflowExecutionResponse{}, err
	}
	return &admin.DescribeWorkflowExecutionResponse{
		ShardId:                common.StringPtr(shardIDForOutput),
		HistoryAddr:            common.StringPtr(historyAddr),
		MutableStateInDatabase: resp2.MutableStateInDatabase,
		MutableStateInCache:    resp2.MutableStateInCache,
	}, err
}

// DescribeHistoryHost returns information about the internal states of a history host
func (adh *AdminHandler) DescribeHistoryHost(ctx context.Context, request *gen.DescribeHistoryHostRequest) (resp *gen.DescribeHistoryHostResponse, retError error) {
	defer logging.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminDescribeHistoryHostScope
	if request == nil || (request.ShardIdForHost == nil && request.ExecutionForHost == nil && request.HostAddress == nil) {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.ExecutionForHost != nil {
		if err := validateExecution(request.ExecutionForHost); err != nil {
			return nil, adh.error(err, scope)
		}
	}

	resp, err := adh.history.DescribeHistoryHost(ctx, request)
	return resp, err
}

// GetWorkflowExecutionRawHistory - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistory(
	ctx context.Context, request *admin.GetWorkflowExecutionRawHistoryRequest) (resp *admin.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer logging.CapturePanic(adh.GetLogger(), &retError)

	scope := metrics.AdminGetWorkflowExecutionRawHistoryScope
	sw := adh.startRequestProfile(scope)
	defer sw.Stop()
	var err error
	var size int

	domainID, err := adh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, adh.error(err, scope)
	}
	domainScope := adh.metricsClient.Scope(scope, metrics.DomainTag(request.GetDomain()))

	execution := request.Execution
	if len(execution.GetWorkflowId()) == 0 {
		return nil, &gen.BadRequestError{Message: "Invalid WorkflowID."}
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if len(execution.GetRunId()) == 0 || uuid.Parse(execution.GetRunId()) == nil {
		return nil, &gen.BadRequestError{Message: "Invalid RunID."}
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize < 0 {
		return nil, &gen.BadRequestError{Message: "Invalid PageSize."}
	}

	var token *getHistoryContinuationToken
	// initialize or validate the token
	// token will be used as a source of truth
	if request.NextPageToken != nil {
		token, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}

		if execution.GetRunId() != token.RunID ||
			// we guarantee to use the first event ID provided in the request
			request.GetFirstEventId() != token.FirstEventID ||
			// the next event ID in the request must be <= next event ID from mutable state, when initialized
			// so as long as customer do not change next event ID during pagination,
			// next event ID in the token <= next event ID in the request.
			request.GetNextEventId() < token.NextEventID {
			return nil, &gen.BadRequestError{Message: "Invalid pagination token."}
		}

		// for the rest variables in the token, since we do not do hmac,
		// the only thing can be done is to trust the token:
		// IsWorkflowRunning: not used
		// TransientDecision: not used
		// PersistenceToken: trust
		// EventStoreVersion: trust
		// ReplicationInfo: trust

	} else {
		firstEventID := request.GetFirstEventId()
		nextEventID := request.GetNextEventId()
		if firstEventID < 0 || firstEventID > nextEventID {
			return nil, &gen.BadRequestError{Message: "Invalid FirstEventID && NextEventID combination."}
		}

		response, err := adh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
			DomainUUID:          common.StringPtr(domainID),
			Execution:           execution,
			ExpectedNextEventId: common.Int64Ptr(common.FirstEventID), // common.FirstEventID means no long poll
		})
		if err != nil {
			return nil, err
		}

		// check if the input next event ID is > actual next event ID in the mutable state
		// since we should not leak invalid events
		if nextEventID > response.GetNextEventId() {
			nextEventID = response.GetNextEventId()
		}
		token = &getHistoryContinuationToken{
			RunID:             execution.GetRunId(),
			BranchToken:       response.BranchToken,
			FirstEventID:      firstEventID,
			NextEventID:       nextEventID,
			PersistenceToken:  nil, // this is the initialized value
			EventStoreVersion: response.GetEventStoreVersion(),
			ReplicationInfo:   response.ReplicationInfo,
		}
	}

	if token.FirstEventID >= token.NextEventID {
		return &admin.GetWorkflowExecutionRawHistoryResponse{
			HistoryBatches:    []*gen.DataBlob{},
			ReplicationInfo:   token.ReplicationInfo,
			EventStoreVersion: common.Int32Ptr(token.EventStoreVersion),
			NextPageToken:     nil, // no further pagination
		}, nil
	}

	// TODO need to deal with transient decision if to be used by client getting history
	var historyBatches []*gen.History
	_, historyBatches, token.PersistenceToken, size, err = historyService.PaginateHistory(
		adh.historyMgr,
		adh.historyV2Mgr,
		adh.metricsClient,
		adh.GetLogger(),
		true, // this means that we are getting history by batch
		domainID,
		execution.GetWorkflowId(),
		token.RunID,
		token.FirstEventID,
		token.NextEventID,
		token.PersistenceToken,
		token.EventStoreVersion,
		token.BranchToken,
		pageSize,
	)
	if err != nil {
		if _, ok := err.(*gen.EntityNotExistsError); ok {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &admin.GetWorkflowExecutionRawHistoryResponse{
				HistoryBatches:    []*gen.DataBlob{},
				ReplicationInfo:   token.ReplicationInfo,
				EventStoreVersion: common.Int32Ptr(token.EventStoreVersion),
				NextPageToken:     nil, // no further pagination
			}, nil
		}
		return nil, err
	}

	// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
	// domains along with the individual domains stats
	adh.metricsClient.RecordTimer(scope, metrics.HistorySize, time.Duration(size))
	domainScope.RecordTimer(metrics.HistorySize, time.Duration(size))

	serializer := persistence.NewHistorySerializer()
	blobs := []*gen.DataBlob{}
	for _, historyBatch := range historyBatches {
		blob, err := serializer.SerializeBatchEvents(historyBatch.Events, common.EncodingTypeThriftRW)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, &gen.DataBlob{
			EncodingType: gen.EncodingTypeThriftRW.Ptr(),
			Data:         blob.Data,
		})
	}

	result := &admin.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:    blobs,
		ReplicationInfo:   token.ReplicationInfo,
		EventStoreVersion: common.Int32Ptr(token.EventStoreVersion),
	}
	if len(token.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = serializeHistoryToken(token)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// startRequestProfile initiates recording of request metrics
func (adh *AdminHandler) startRequestProfile(scope int) metrics.Stopwatch {
	adh.startWG.Wait()
	sw := adh.metricsClient.StartTimer(scope, metrics.CadenceLatency)
	adh.metricsClient.IncCounter(scope, metrics.CadenceRequests)
	return sw
}

func (adh *AdminHandler) error(err error, scope int) error {
	switch err.(type) {
	case *gen.InternalServiceError:
		logging.LogInternalServiceError(adh.Service.GetLogger(), err)
		return err
	case *gen.BadRequestError:
		return err
	case *gen.ServiceBusyError:
		return err
	case *gen.EntityNotExistsError:
		return err
	default:
		logging.LogUncategorizedError(adh.Service.GetLogger(), err)
		return &gen.InternalServiceError{Message: err.Error()}
	}
}
