package stress

import (
	"sync"
	"time"

	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"code.uber.internal/go-common.git/x/log"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/health/driver"
	"code.uber.internal/devexp/minions/workflow"
)

// Host is created for each host running the stress test
type Host struct {
	hostName string
	config   Configuration
	engine   workflow.Engine
	reporter common.Reporter

	instancesWG            sync.WaitGroup
	doneCh                 chan struct{}
	runOnMinionsProduction bool
}

var stressMetrics = map[common.MetricName]common.MetricType{
	common.WorkflowsStartTotalCounter:      common.Counter,
	common.WorkflowsCompletionTotalCounter: common.Counter,
	common.ActivitiesTotalCounter:          common.Counter,
	common.DecisionsTotalCounter:           common.Counter,
	common.WorkflowEndToEndLatency:         common.Timer,
	common.ActivityEndToEndLatency:         common.Timer,
	common.DecisionsEndToEndLatency:        common.Timer,
}

// NewStressHost creates an instance of stress host
func NewStressHost(engine workflow.Engine, instanceName string, config Configuration,
	reporter common.Reporter, runOnMinionsProduction bool) *Host {
	h := &Host{
		engine:                 engine,
		hostName:               instanceName,
		config:                 config,
		reporter:               reporter,
		doneCh:                 make(chan struct{}),
		runOnMinionsProduction: runOnMinionsProduction,
	}

	h.reporter.InitMetrics(stressMetrics)
	return h
}

// GetThriftClient gets thrift client.
func (s *Host) GetThriftClient(tchan *tchannel.Channel) m.TChanWorkflowService {
	tclient := thrift.NewClient(tchan, "uber-minions", nil)
	return m.NewTChanWorkflowServiceClient(tclient)
}

// Start is used the start the stress host
func (s *Host) Start() {
	launchCh := make(chan struct{})

	workflowConfig := s.config.StressConfig.WorkflowConfig
	log.Infof("Launching stress workflow with configuration: %+v", workflowConfig)

	go func() {

		var service m.TChanWorkflowService

		if s.runOnMinionsProduction {
			// TChannel to production.
			tchan, err := s.config.TChannel.NewClient("stress-client", nil)
			if err != nil {
				log.Panicf("Failed to get a client for the uber-minions: %s\n", err.Error())
			}
			service = s.GetThriftClient(tchan)
		} else {
			serviceMockEngine := driver.NewServiceMockEngine(s.engine)
			serviceMockEngine.Start()
			service = serviceMockEngine
		}

		workflowPrams := &driver.WorkflowParams{
			ChainSequence:    workflowConfig.ChainSequence,
			ActivitySleepMin: time.Duration(workflowConfig.ActivitySleepMin) * time.Second,
			ActivitySleepMax: time.Duration(workflowConfig.ActivitySleepMax) * time.Second}

		driver.LaunchWorkflows(
			workflowConfig.TotalLaunchCount,
			workflowConfig.RoutineCount,
			workflowPrams,
			service,
			s.reporter)

		// close(launchCh)
	}()

	if _, ok := s.reporter.(*common.SimpleReporter); ok {
		go s.printMetric()
	}

	<-launchCh

	close(s.doneCh)
}

func (s *Host) printMetric() {
	sr, ok := s.reporter.(*common.SimpleReporter)
	if !ok {
		log.Error("Invalid reporter passed to printMetric.")
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sr.PrintStressMetric()
			sr.PrintFinalMetric()
			if sr.IsProcessComplete() {
				sr.PrintFinalMetric()
				return
			}

		case <-s.doneCh:
			return
		}
	}
}
