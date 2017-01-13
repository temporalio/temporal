package host

import (
	"sync"
	"time"

	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"
	"code.uber.internal/devexp/minions/service/frontend"
	"code.uber.internal/devexp/minions/service/history"
	"code.uber.internal/devexp/minions/service/matching"
	"github.com/uber-common/bark"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// Cadence hosts all of cadence services in one process
type Cadence interface {
	Start() error
	Stop()
	FrontendAddress() string
	MatchingServiceAddress() string
	HistoryServiceAddress() string
}

type cadenceImpl struct {
	frontendHandler *frontend.WorkflowHandler
	matchingHandler *matching.Handler
	historyHandler  *history.Handler
	logger          bark.Logger
	taskMgr         persistence.TaskManager
	executionMgr    persistence.ExecutionManager
	shutdownCh      chan struct{}
}

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(executionMgr persistence.ExecutionManager, taskMgr persistence.TaskManager, logger bark.Logger) Cadence {
	return &cadenceImpl{
		logger:       logger,
		taskMgr:      taskMgr,
		executionMgr: executionMgr,
		shutdownCh:   make(chan struct{}),
	}
}

func (c *cadenceImpl) Start() error {
	var rpHosts []string
	rpHosts = append(rpHosts, c.FrontendAddress())
	rpHosts = append(rpHosts, c.MatchingServiceAddress())
	rpHosts = append(rpHosts, c.HistoryServiceAddress())

	var startWG sync.WaitGroup
	startWG.Add(3)

	go c.startFrontend(c.logger, rpHosts, &startWG)
	go c.startHistory(c.logger, c.executionMgr, c.taskMgr, rpHosts, &startWG)
	go c.startMatching(c.logger, c.taskMgr, rpHosts, &startWG)

	startWG.Wait()
	// Allow some time for the ring to stabilize
	// TODO: remove this after adding automatic retries on transient errors in clients
	time.Sleep(time.Second * 10)
	return nil
}

func (c *cadenceImpl) Stop() {
	c.frontendHandler.Stop()
	c.historyHandler.Stop()
	c.matchingHandler.Stop()
	close(c.shutdownCh)
}

func (c *cadenceImpl) FrontendAddress() string {
	return "127.0.0.1:7104"
}

func (c *cadenceImpl) HistoryServiceAddress() string {
	return "127.0.0.1:7105"
}

func (c *cadenceImpl) MatchingServiceAddress() string {
	return "127.0.0.1:7106"
}

func (c *cadenceImpl) startFrontend(logger bark.Logger, rpHosts []string, startWG *sync.WaitGroup) {
	tchanFactory := func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {
		return c.createTChannel(sName, c.FrontendAddress(), thriftServices)
	}
	service := common.NewService("cadence-frontend", logger, tchanFactory, rpHosts)
	var thriftServices []thrift.TChanServer
	c.frontendHandler, thriftServices = frontend.NewWorkflowHandler(service)
	err := c.frontendHandler.Start(thriftServices)
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to start frontend")
	}
	startWG.Done()
	<-c.shutdownCh
}

func (c *cadenceImpl) startHistory(logger bark.Logger, executionMgr persistence.ExecutionManager,
	taskMgr persistence.TaskManager, rpHosts []string, startWG *sync.WaitGroup) {
	tchanFactory := func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {
		return c.createTChannel(sName, c.HistoryServiceAddress(), thriftServices)
	}
	service := common.NewService("cadence-history", logger, tchanFactory, rpHosts)
	var thriftServices []thrift.TChanServer
	c.historyHandler, thriftServices = history.NewHandler(service, executionMgr, taskMgr)
	c.historyHandler.Start(thriftServices)
	startWG.Done()
	<-c.shutdownCh
}

func (c *cadenceImpl) startMatching(logger bark.Logger, taskMgr persistence.TaskManager,
	rpHosts []string, startWG *sync.WaitGroup) {
	tchanFactory := func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {
		return c.createTChannel(sName, c.MatchingServiceAddress(), thriftServices)
	}
	service := common.NewService("cadence-matching", logger, tchanFactory, rpHosts)
	var thriftServices []thrift.TChanServer
	c.matchingHandler, thriftServices = matching.NewHandler(taskMgr, service)
	c.matchingHandler.Start(thriftServices)
	startWG.Done()
	<-c.shutdownCh
}

func (c *cadenceImpl) createTChannel(sName string, hostPort string,
	thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {
	ch, err := tchannel.NewChannel(sName, nil)
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to create TChannel")
	}
	server := thrift.NewServer(ch)
	for _, thriftService := range thriftServices {
		server.Register(thriftService)
	}

	err = ch.ListenAndServe(hostPort)
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to listen on tchannel")
	}
	return ch, server
}
