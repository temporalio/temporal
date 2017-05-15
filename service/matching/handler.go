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

package matching

import (
	"sync"

	m "github.com/uber/cadence/.gen/go/matching"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/tchannel-go/thrift"
)

var _ m.TChanMatchingService = (*Handler)(nil)

// Handler - Thrift handler inteface for history service
type Handler struct {
	taskPersistence persistence.TaskManager
	engine          Engine
	startWG         sync.WaitGroup
	service.Service
}

// NewHandler creates a thrift handler for the history service
func NewHandler(taskPersistence persistence.TaskManager, sVice service.Service) (*Handler, []thrift.TChanServer) {
	handler := &Handler{
		Service:         sVice,
		taskPersistence: taskPersistence,
	}
	// prevent us from trying to serve requests before matching engine is started and ready
	handler.startWG.Add(1)
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
	h.startWG.Done()
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
	h.Service.GetLogger().Debug("Engine Received AddActivityTask")
	h.startWG.Wait()
	return h.engine.AddActivityTask(addRequest)
}

// AddDecisionTask - adds a decision task.
func (h *Handler) AddDecisionTask(ctx thrift.Context, addRequest *m.AddDecisionTaskRequest) error {
	h.Service.GetLogger().Debug("Engine Received AddDecisionTask")
	h.startWG.Wait()
	return h.engine.AddDecisionTask(addRequest)
}

// PollForActivityTask - long poll for an activity task.
func (h *Handler) PollForActivityTask(ctx thrift.Context,
	pollRequest *m.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {
	h.Service.GetLogger().Debug("Engine Received PollForActivityTask")
	h.startWG.Wait()
	response, error := h.engine.PollForActivityTask(ctx, pollRequest)
	h.Service.GetLogger().Debug("Engine returned from PollForActivityTask")
	return response, error

}

// PollForDecisionTask - long poll for a decision task.
func (h *Handler) PollForDecisionTask(ctx thrift.Context,
	pollRequest *m.PollForDecisionTaskRequest) (*m.PollForDecisionTaskResponse, error) {
	h.Service.GetLogger().Debug("Engine Received PollForDecisionTask")
	h.startWG.Wait()
	response, error := h.engine.PollForDecisionTask(ctx, pollRequest)
	h.Service.GetLogger().Debug("Engine returned from PollForDecisionTask")
	return response, error
}
