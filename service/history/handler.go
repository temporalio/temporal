package history

import (
	"fmt"
	"log"
	"sync"

	hist "github.com/uber/cadence/.gen/go/history"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/tchannel-go/thrift"
)

// Handler - Thrift handler inteface for history service
type Handler struct {
	numberOfShards        int
	shardManager          persistence.ShardManager
	executionMgrFactory   persistence.ExecutionManagerFactory
	matchingServiceClient matching.Client
	hServiceResolver      membership.ServiceResolver
	controller            *shardController
	tokenSerializer       common.TaskTokenSerializer
	startWG               sync.WaitGroup
	service.Service
}

var _ hist.TChanHistoryService = (*Handler)(nil)
var _ EngineFactory = (*Handler)(nil)

// NewHandler creates a thrift handler for the history service
func NewHandler(sVice service.Service, shardManager persistence.ShardManager,
	executionMgrFactory persistence.ExecutionManagerFactory, numberOfShards int) (*Handler, []thrift.TChanServer) {
	handler := &Handler{
		Service:             sVice,
		shardManager:        shardManager,
		executionMgrFactory: executionMgrFactory,
		numberOfShards:      numberOfShards,
		tokenSerializer:     common.NewJSONTaskTokenSerializer(),
	}
	// prevent us from trying to serve requests before shard controller is started and ready
	handler.startWG.Add(1)
	return handler, []thrift.TChanServer{hist.NewTChanHistoryServiceServer(handler)}
}

// Start starts the handler
func (h *Handler) Start(thriftService []thrift.TChanServer) error {
	h.Service.Start(thriftService)
	matchingServiceClient, err0 := h.Service.GetClientFactory().NewMatchingClient()
	if err0 != nil {
		return err0
	}
	h.matchingServiceClient = matchingServiceClient

	hServiceResolver, err1 := h.GetMembershipMonitor().GetResolver(common.HistoryServiceName)
	if err1 != nil {
		h.Service.GetLogger().Fatalf("Unable to get history service resolver.")
	}
	h.hServiceResolver = hServiceResolver
	h.controller = newShardController(h.numberOfShards, h.GetHostInfo(), hServiceResolver, h.shardManager,
		h.executionMgrFactory, h, h.GetLogger(), h.GetMetricsClient())
	h.controller.Start()
	h.startWG.Done()
	return nil
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.controller.Stop()
	h.Service.Stop()
}

// CreateEngine is implementation for HistoryEngineFactory used for creating the engine instance for shard
func (h *Handler) CreateEngine(context ShardContext) Engine {
	return NewEngineWithShardContext(context, h.matchingServiceClient)
}

// IsHealthy - Health endpoint.
func (h *Handler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	h.startWG.Wait()
	token, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.GetTaskToken())
	if err0 != nil {
		return nil, &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return nil, err1
	}

	response, err2 := engine.RecordActivityTaskHeartbeat(heartbeatRequest)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx thrift.Context,
	recordRequest *hist.RecordActivityTaskStartedRequest) (*hist.RecordActivityTaskStartedResponse, error) {
	h.startWG.Wait()
	workflowExecution := recordRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}

	response, err2 := engine.RecordActivityTaskStarted(recordRequest)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RecordDecisionTaskStarted - Record Decision Task started.
func (h *Handler) RecordDecisionTaskStarted(ctx thrift.Context,
	recordRequest *hist.RecordDecisionTaskStartedRequest) (*hist.RecordDecisionTaskStartedResponse, error) {
	h.startWG.Wait()
	h.Service.GetLogger().Debugf("RecordDecisionTaskStarted. WorkflowID: %v, RunID: %v, ScheduleID: %v",
		recordRequest.GetWorkflowExecution().GetWorkflowId(),
		recordRequest.GetWorkflowExecution().GetRunId(),
		recordRequest.GetScheduleId())
	workflowExecution := recordRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.Service.GetLogger().Errorf("RecordDecisionTaskStarted failed. Error: %v. WorkflowID: %v, RunID: %v, ScheduleID: %v",
			err1,
			recordRequest.GetWorkflowExecution().GetWorkflowId(),
			recordRequest.GetWorkflowExecution().GetRunId(),
			recordRequest.GetScheduleId())
		return nil, err1
	}

	response, err2 := engine.RecordDecisionTaskStarted(recordRequest)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	h.startWG.Wait()
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}

	err2 := engine.RespondActivityTaskCompleted(completeRequest)
	if err2 != nil {
		return h.convertError(err2)
	}

	return nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx thrift.Context,
	failRequest *gen.RespondActivityTaskFailedRequest) error {
	h.startWG.Wait()
	token, err0 := h.tokenSerializer.Deserialize(failRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}

	err2 := engine.RespondActivityTaskFailed(failRequest)
	if err2 != nil {
		return h.convertError(err2)
	}

	return nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx thrift.Context,
	cancelRequest *gen.RespondActivityTaskCanceledRequest) error {
	h.startWG.Wait()
	token, err0 := h.tokenSerializer.Deserialize(cancelRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}

	err2 := engine.RespondActivityTaskCanceled(cancelRequest)
	if err2 != nil {
		return h.convertError(err2)
	}

	return nil
}

// RespondDecisionTaskCompleted - records completion of a decision task
func (h *Handler) RespondDecisionTaskCompleted(ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	h.startWG.Wait()
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	h.Service.GetLogger().Debugf("RespondDecisionTaskCompleted. WorkflowID: %v, RunID: %v, ScheduleID: %v",
		token.WorkflowID,
		token.RunID,
		token.ScheduleID)

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}

	err2 := engine.RespondDecisionTaskCompleted(completeRequest)
	if err2 != nil {
		return h.convertError(err2)
	}

	return nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	h.startWG.Wait()
	engine, err1 := h.controller.GetEngine(startRequest.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}

	response, err2 := engine.StartWorkflowExecution(startRequest)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// GetWorkflowExecutionHistory - returns the complete history of a workflow execution
func (h *Handler) GetWorkflowExecutionHistory(ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	h.startWG.Wait()
	workflowExecution := getRequest.GetExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}

	return engine.GetWorkflowExecutionHistory(getRequest)
}

// convertError is a helper method to convert ShardOwnershipLostError from persistence layer returned by various
// HistoryEngine API calls to ShardOwnershipLost error return by HistoryService for client to be redirected to the
// correct shard.
func (h *Handler) convertError(err error) error {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		shardID := err.(*persistence.ShardOwnershipLostError).ShardID
		info, err := h.hServiceResolver.Lookup(string(shardID))
		if err != nil {
			return createShardOwnershipLostError(h.GetHostInfo().GetAddress(), info.GetAddress())
		}
		return createShardOwnershipLostError(h.GetHostInfo().GetAddress(), "")
	}

	return err
}

func createShardOwnershipLostError(currentHost, ownerHost string) *hist.ShardOwnershipLostError {
	shardLostErr := hist.NewShardOwnershipLostError()
	shardLostErr.Message = common.StringPtr(fmt.Sprintf("Shard is not owned by host: %v", currentHost))
	shardLostErr.Owner = common.StringPtr(ownerHost)

	return shardLostErr
}
