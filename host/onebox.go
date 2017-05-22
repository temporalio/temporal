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

package host

import (
	"fmt"
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/frontend"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
	ringpop "github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const rpAppNamePrefix string = "cadence"
const maxRpJoinTimeout = 30 * time.Second

// Cadence hosts all of cadence services in one process
type Cadence interface {
	Start() error
	Stop()
	FrontendAddress() string
	MatchingServiceAddress() string
	HistoryServiceAddress() []string
}

type (
	cadenceImpl struct {
		frontendHandler       *frontend.WorkflowHandler
		matchingHandler       *matching.Handler
		historyHandlers       []*history.Handler
		numberOfHistoryShards int
		numberOfHistoryHosts  int
		logger                bark.Logger
		metadataMgr           persistence.MetadataManager
		shardMgr              persistence.ShardManager
		historyMgr            persistence.HistoryManager
		taskMgr               persistence.TaskManager
		visibilityMgr         persistence.VisibilityManager
		executionMgrFactory   persistence.ExecutionManagerFactory
		shutdownCh            chan struct{}
		shutdownWG            sync.WaitGroup
	}

	ringpopFactoryImpl struct {
		service string
		rpHosts []string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(metadataMgr persistence.MetadataManager, shardMgr persistence.ShardManager,
	historyMgr persistence.HistoryManager, executionMgrFactory persistence.ExecutionManagerFactory,
	taskMgr persistence.TaskManager, visibilityMgr persistence.VisibilityManager,
	numberOfHistoryShards, numberOfHistoryHosts int, logger bark.Logger) Cadence {
	return &cadenceImpl{
		numberOfHistoryShards: numberOfHistoryShards,
		numberOfHistoryHosts:  numberOfHistoryHosts,
		logger:                logger,
		metadataMgr:           metadataMgr,
		visibilityMgr:         visibilityMgr,
		shardMgr:              shardMgr,
		historyMgr:            historyMgr,
		taskMgr:               taskMgr,
		executionMgrFactory:   executionMgrFactory,
		shutdownCh:            make(chan struct{}),
	}
}

func (c *cadenceImpl) Start() error {
	var rpHosts []string
	rpHosts = append(rpHosts, c.FrontendAddress())
	rpHosts = append(rpHosts, c.MatchingServiceAddress())
	rpHosts = append(rpHosts, c.HistoryServiceAddress()...)

	var startWG sync.WaitGroup
	startWG.Add(2)
	go c.startHistory(c.logger, c.shardMgr, c.metadataMgr, c.visibilityMgr, c.historyMgr, c.executionMgrFactory, rpHosts, &startWG)
	go c.startMatching(c.logger, c.taskMgr, rpHosts, &startWG)
	startWG.Wait()

	startWG.Add(1)
	go c.startFrontend(c.logger, rpHosts, &startWG)
	startWG.Wait()
	return nil
}

func (c *cadenceImpl) Stop() {
	c.shutdownWG.Add(3)
	c.frontendHandler.Stop()
	for _, historyHandler := range c.historyHandlers {
		historyHandler.Stop()
	}
	c.matchingHandler.Stop()
	close(c.shutdownCh)
	c.shutdownWG.Wait()
}

func (c *cadenceImpl) FrontendAddress() string {
	return "127.0.0.1:7104"
}

func (c *cadenceImpl) HistoryServiceAddress() []string {
	hosts := []string{}
	startPort := 7200
	for i := 0; i < c.numberOfHistoryHosts; i++ {
		port := startPort + i
		hosts = append(hosts, fmt.Sprintf("127.0.0.1:%v", port))
	}

	c.logger.Infof("History hosts: %v", hosts)
	return hosts
}

func (c *cadenceImpl) MatchingServiceAddress() string {
	return "127.0.0.1:7106"
}

func (c *cadenceImpl) startFrontend(logger bark.Logger, rpHosts []string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.Name = common.FrontendServiceName
	params.Logger = logger
	params.TChannelFactory = newTChannelFactory(c.FrontendAddress(), logger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(common.FrontendServiceName, rpHosts)
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	params.CassandraConfig.Hosts = "127.0.0.1"
	service := service.New(params)
	var thriftServices []thrift.TChanServer
	c.frontendHandler, thriftServices = frontend.NewWorkflowHandler(service, c.metadataMgr, c.historyMgr, c.visibilityMgr)
	err := c.frontendHandler.Start(thriftServices)
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to start frontend")
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startHistory(logger bark.Logger, shardMgr persistence.ShardManager,
	metadataMgr persistence.MetadataManager, visibilityMgr persistence.VisibilityManager, historyMgr persistence.HistoryManager,
	executionMgrFactory persistence.ExecutionManagerFactory, rpHosts []string, startWG *sync.WaitGroup) {

	for _, hostport := range c.HistoryServiceAddress() {
		params := new(service.BootstrapParams)
		params.Name = common.HistoryServiceName
		params.Logger = logger
		params.TChannelFactory = newTChannelFactory(hostport, logger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.RingpopFactory = newRingpopFactory(common.FrontendServiceName, rpHosts)
		params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
		service := service.New(params)
		var thriftServices []thrift.TChanServer
		var handler *history.Handler
		handler, thriftServices = history.NewHandler(service, shardMgr, metadataMgr, visibilityMgr, historyMgr, executionMgrFactory,
			c.numberOfHistoryShards)
		handler.Start(thriftServices)
		c.historyHandlers = append(c.historyHandlers, handler)
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startMatching(logger bark.Logger, taskMgr persistence.TaskManager,
	rpHosts []string, startWG *sync.WaitGroup) {

	params := new(service.BootstrapParams)
	params.Name = common.MatchingServiceName
	params.Logger = logger

	params.TChannelFactory = newTChannelFactory(c.MatchingServiceAddress(), logger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(common.FrontendServiceName, rpHosts)
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	service := service.New(params)
	var thriftServices []thrift.TChanServer
	c.matchingHandler, thriftServices = matching.NewHandler(taskMgr, service)
	c.matchingHandler.Start(thriftServices)
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func newRingpopFactory(service string, rpHosts []string) service.RingpopFactory {
	return &ringpopFactoryImpl{
		service: service,
		rpHosts: rpHosts,
	}
}

func (p *ringpopFactoryImpl) CreateRingpop(ch *tchannel.Channel) (*ringpop.Ringpop, error) {
	rp, err := ringpop.New(fmt.Sprintf("%s", rpAppNamePrefix), ringpop.Channel(ch))
	if err != nil {
		return nil, err
	}
	err = p.bootstrapRingpop(rp, p.rpHosts)
	if err != nil {
		return nil, err
	}
	return rp, nil
}

// bootstrapRingpop tries to bootstrap the given ringpop instance using the hosts list
func (p *ringpopFactoryImpl) bootstrapRingpop(rp *ringpop.Ringpop, rpHosts []string) error {
	// TODO: log ring hosts

	bOptions := new(swim.BootstrapOptions)
	bOptions.DiscoverProvider = statichosts.New(rpHosts...)
	bOptions.MaxJoinDuration = maxRpJoinTimeout
	bOptions.JoinSize = 1 // this ensures the first guy comes up quickly

	_, err := rp.Bootstrap(bOptions)
	return err
}

type tchannelFactoryImpl struct {
	hostPort string
	logger   bark.Logger
}

func newTChannelFactory(hostPort string, logger bark.Logger) service.TChannelFactory {
	return &tchannelFactoryImpl{
		hostPort: hostPort,
		logger:   logger,
	}
}

func (c *tchannelFactoryImpl) CreateChannel(sName string,
	thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {

	ch, err := tchannel.NewChannel(sName, nil)
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to create TChannel")
	}
	server := thrift.NewServer(ch)
	for _, thriftService := range thriftServices {
		server.Register(thriftService)
	}

	err = ch.ListenAndServe(c.hostPort)
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to listen on tchannel")
	}
	return ch, server
}
