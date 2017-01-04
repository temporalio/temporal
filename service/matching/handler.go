package matching

import (
	m "code.uber.internal/devexp/minions/.gen/go/matching"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/persistence"
	"code.uber.internal/devexp/minions/workflow"
	"github.com/uber/tchannel-go/thrift"
)

var _ m.TChanMatchingService = (*Handler)(nil)

// Handler - Thrift handler inteface for history service
type Handler struct {
	taskPersistence persistence.TaskManager
	engine          workflow.MatchingEngine
	common.Service
}

// NewHandler creates a thrift handler for the history service
func NewHandler(taskPersistence persistence.TaskManager, sVice common.Service) (*Handler, []thrift.TChanServer) {
	handler := &Handler{
		Service:         sVice,
		taskPersistence: taskPersistence,
	}
	return handler, []thrift.TChanServer{m.NewTChanMatchingServiceServer(handler)}
}

// Start starts the handler
func (h *Handler) Start(thriftService []thrift.TChanServer) error {
	h.Service.Start(thriftService)
	history, err := h.Service.GetClientFactory().NewHistoryClient()
	if err != nil {
		return err
	}
	h.engine = workflow.NewMatchingEngine(h.taskPersistence, history, h.Service.GetLogger())
	return nil
}

// IsHealthy - Health endpoint.
func (h *Handler) IsHealthy(ctx thrift.Context) (bool, error) {
	h.Service.GetLogger().Info("Workflow Health endpoint reached.")
	return true, nil
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(ctx thrift.Context, addRequest *m.AddActivityTaskRequest) error {
	// Note: this API is needed so the transfer queue can add tasks without writing them directly to the DB.
	panic("not implemented")
}

// AddDecisionTask - adds a decision task.
func (h *Handler) AddDecisionTask(ctx thrift.Context, addRequest *m.AddDecisionTaskRequest) error {
	// Note: this API is needed so the transfer queue can add tasks without writing them directly to the DB.
	panic("not implemented")
}

// PollForActivityTask - long poll for an activity task.
func (h *Handler) PollForActivityTask(ctx thrift.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {
	return h.engine.PollForActivityTask(pollRequest)
}

// PollForDecisionTask - long poll for a decision task.
func (h *Handler) PollForDecisionTask(ctx thrift.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {
	return h.engine.PollForDecisionTask(pollRequest)
}
