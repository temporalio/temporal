package service

import (
	"log"

	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/config"
)

// WorkflowServer is the interface that each protocol should implement
type WorkflowServer interface {
	Start()
	Stop()
}

// TChannelWorkflowServer is the TChannel interface of the workflow service
type TChannelWorkflowServer struct {
}

// Start - that starts the workflow service
func (s *TChannelWorkflowServer) Start(appConfig *config.AppConfig) error {

	registerHandlers := func(ch *tchannel.Channel, server *thrift.Server) {
		server.Register(minions.NewTChanWorkflowServiceServer(WorkflowHandler{}))
	}

	_, err := appConfig.TChannel.New(appConfig.ServiceName, nil /* metrics */, registerHandlers)
	if err != nil {
		log.Fatalf("TChannel.New failed: %v", err)
		return err
	}

	return nil
}

// NewTChannelWorkflowServer - creates a workflow server instance
func NewTChannelWorkflowServer() *TChannelWorkflowServer {
	return &TChannelWorkflowServer{}
}
