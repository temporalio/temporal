package history

import (
	"log"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"
	"github.com/uber/tchannel-go/thrift"
)

const (
	// TODO: Move this to config knob
	numberOfShards = 1
)

// Handler - Thrift handler inteface for history service
type Handler struct {
	numberOfShards        int
	shardManager          persistence.ShardManager
	executionManager      persistence.ExecutionManager
	matchingServiceClient matching.Client
	controller            *shardController
	tokenSerializer       common.TaskTokenSerializer
	common.Service
}

var _ h.TChanHistoryService = (*Handler)(nil)
var _ EngineFactory = (*Handler)(nil)

// NewHandler creates a thrift handler for the history service
func NewHandler(sVice common.Service, shardManager persistence.ShardManager,
	executionPersistence persistence.ExecutionManager) (*Handler, []thrift.TChanServer) {
	handler := &Handler{
		Service:          sVice,
		shardManager:     shardManager,
		executionManager: executionPersistence,
		numberOfShards:   numberOfShards,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
	}
	return handler, []thrift.TChanServer{h.NewTChanHistoryServiceServer(handler)}
}

// Start starts the handler
func (h *Handler) Start(thriftService []thrift.TChanServer) error {
	h.Service.Start(thriftService)
	matchingServiceClient, err0 := h.Service.GetClientFactory().NewMatchingClient()
	if err0 != nil {
		return err0
	}
	h.matchingServiceClient = matchingServiceClient

	hServiceResolver, err1 := h.GetMembershipMonitor().GetResolver("cadence-history")
	if err1 != nil {
		h.Service.GetLogger().Fatalf("Unable to get history service resolver.")
	}
	h.controller = newShardController(h.numberOfShards, h.GetHostInfo(), hServiceResolver, h.shardManager,
		h, h.GetLogger())
	h.controller.Start()
	return nil
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.controller.Stop()
	h.Service.Stop()
}

// CreateEngine is implementation for HistoryEngineFactory used for creating the engine instance for shard
func (h *Handler) CreateEngine(context ShardContext) Engine {
	return NewEngineWithShardContext(context, h.executionManager, h.matchingServiceClient, h.Service.GetLogger())
}

// IsHealthy - Health endpoint.
func (h *Handler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	token, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.GetTaskToken())
	if err0 != nil {
		return nil, &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return nil, err1
	}

	return engine.RecordActivityTaskHeartbeat(heartbeatRequest)
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx thrift.Context,
	recordRequest *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	workflowExecution := recordRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}

	return engine.RecordActivityTaskStarted(recordRequest)
}

// RecordDecisionTaskStarted - Record Decision Task started.
func (h *Handler) RecordDecisionTaskStarted(ctx thrift.Context,
	recordRequest *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	workflowExecution := recordRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}

	return engine.RecordDecisionTaskStarted(recordRequest)
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}

	return engine.RespondActivityTaskCompleted(completeRequest)
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx thrift.Context,
	failRequest *gen.RespondActivityTaskFailedRequest) error {
	token, err0 := h.tokenSerializer.Deserialize(failRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}
	return engine.RespondActivityTaskFailed(failRequest)
}

// RespondDecisionTaskCompleted - records completion of a decision task
func (h *Handler) RespondDecisionTaskCompleted(ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err0 != nil {
		return &gen.BadRequestError{Message: "Error deserializing task token."}
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		return err1
	}
	return engine.RespondDecisionTaskCompleted(completeRequest)
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	engine, err1 := h.controller.GetEngine(startRequest.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}
	return engine.StartWorkflowExecution(startRequest)
}

// GetWorkflowExecutionHistory - returns the complete history of a workflow execution
func (h *Handler) GetWorkflowExecutionHistory(ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	workflowExecution := getRequest.GetExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		return nil, err1
	}

	return engine.GetWorkflowExecutionHistory(getRequest)
}
