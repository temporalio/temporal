package matching

import (
	m "code.uber.internal/devexp/minions/.gen/go/matching"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"
	"github.com/uber/tchannel-go/thrift"
)

var _ m.TChanMatchingService = (*Handler)(nil)

// Handler - Thrift handler inteface for history service
type Handler struct {
	taskPersistence persistence.TaskManager
	engine          Engine
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
	h.engine = NewEngine(h.taskPersistence, history, h.Service.GetLogger())
	return nil
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.Service.Stop()
}

// IsHealthy - Health endpoint.
func (h *Handler) IsHealthy(ctx thrift.Context) (bool, error) {
	h.Service.GetLogger().Info("Workflow Health endpoint reached.")
	return true, nil
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(ctx thrift.Context, addRequest *m.AddActivityTaskRequest) error {
	return h.engine.AddActivityTask(addRequest)
}

// AddDecisionTask - adds a decision task.
func (h *Handler) AddDecisionTask(ctx thrift.Context, addRequest *m.AddDecisionTaskRequest) error {
	return h.engine.AddDecisionTask(addRequest)
}

// PollForActivityTask - long poll for an activity task.
func (h *Handler) PollForActivityTask(ctx thrift.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {
	h.Service.GetLogger().Debug("Engine Received PollForActivityTask")
	return h.engine.PollForActivityTask(pollRequest)
}

// PollForDecisionTask - long poll for a decision task.
func (h *Handler) PollForDecisionTask(ctx thrift.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {
	h.Service.GetLogger().Debug("Engine Received PollForDecisionTask")
	return h.engine.PollForDecisionTask(pollRequest)
}
