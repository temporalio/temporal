package history

import (
	"log"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/workflow"
	"github.com/uber/tchannel-go/thrift"
)

var _ h.TChanHistoryService = (*Handler)(nil)

// Handler - Thrift handler inteface for history service
type Handler struct {
	engine workflow.HistoryEngine
	common.Service
}

// NewHandler creates a thrift handler for the history service
func NewHandler(engine workflow.HistoryEngine, sVice common.Service) (*Handler, []thrift.TChanServer) {
	handler := &Handler{
		Service: sVice,
		engine:  engine,
	}
	return handler, []thrift.TChanServer{h.NewTChanHistoryServiceServer(handler)}
}

// Start starts the handler
func (h *Handler) Start(thriftService []thrift.TChanServer) {
	h.engine.Start()
	h.Service.Start(thriftService)
}

// IsHealthy - Health endpoint.
func (h *Handler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	panic("Not Implemented")
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx thrift.Context,
	recordRequest *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	return h.engine.RecordActivityTaskStarted(recordRequest)
}

// RecordDecisionTaskStarted - Record Decision Task started.
func (h *Handler) RecordDecisionTaskStarted(ctx thrift.Context,
	recordRequest *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	return h.engine.RecordDecisionTaskStarted(recordRequest)
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	return h.engine.RespondActivityTaskCompleted(completeRequest)
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx thrift.Context,
	failRequest *gen.RespondActivityTaskFailedRequest) error {
	return h.engine.RespondActivityTaskFailed(failRequest)
}

// RespondDecisionTaskCompleted - records completion of a decision task
func (h *Handler) RespondDecisionTaskCompleted(ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	return h.engine.RespondDecisionTaskCompleted(completeRequest)
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	return h.engine.StartWorkflowExecution(startRequest)
}

// GetWorkflowExecutionHistory - returns the complete history of a workflow execution
func (h *Handler) GetWorkflowExecutionHistory(ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	return h.engine.GetWorkflowExecutionHistory(getRequest)
}
