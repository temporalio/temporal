package stress

import (
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/health/driver"
)

// Host is created for each host running the stress test
type Host struct {
	hostName          string
	config            Configuration
	cassandraHostName string
	reporter          common.Reporter

	instancesWG sync.WaitGroup
	doneCh      chan struct{}
}

var stressMetrics = map[common.MetricName]common.MetricType{
	common.WorkflowsStartTotalCounter:      common.Counter,
	common.WorkflowsCompletionTotalCounter: common.Counter,
	common.ActivitiesTotalCounter:          common.Counter,
	common.DecisionsTotalCounter:           common.Counter,
	common.WorkflowEndToEndLatency:         common.Timer,
}

// NewStressHost creates an instance of stress host
func NewStressHost(cassandraHostName string, instanceName string, config Configuration, reporter common.Reporter) *Host {
	h := &Host{
		cassandraHostName: cassandraHostName,
		hostName:          instanceName,
		config:            config,
		reporter:          reporter,
		doneCh:            make(chan struct{}),
	}

	h.reporter.InitMetrics(stressMetrics)
	return h
}

// Start is used the start the stress host
func (s *Host) Start() {
	launchCh := make(chan struct{})

	workflowConfig := s.config.StressConfig.WorkflowConfig
	log.Infof("Launching stress workflow with configuration: %+v", workflowConfig)

	go func() {
		service := driver.NewServiceMockEngine(s.cassandraHostName)
		service.Start()

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
			if sr.IsProcessComplete() {
				sr.PrintFinalMetric()
				return
			}

		case <-s.doneCh:
			return
		}
	}
}
