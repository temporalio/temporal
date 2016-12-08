package main

import (
	"flag"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/uber/tchannel-go"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/devexp/minions/common"
	s "code.uber.internal/devexp/minions/health/stress"
	"code.uber.internal/devexp/minions/persistence"
	"code.uber.internal/devexp/minions/workflow"
	wmetrics "code.uber.internal/devexp/minions/workflow/metrics"
)

func main() {
	var host string
	var emitMetric string
	var runOnMinionsProduction bool

	flag.StringVar(&host, "host", "", "Cassandra host to use.")
	flag.StringVar(&emitMetric, "emitMetric", "local", "Metric source: m3 | local")
	flag.BoolVar(&runOnMinionsProduction, "runOnMinionsProduction", false, "Run against the minions production")

	flag.Parse()

	log.Info("Starting stress.")
	var cfg s.Configuration
	if err := config.Load(&cfg); err != nil {
		log.WithField(common.TagErr, err).Fatal(`Error initializing configuration`)
	}
	log.Configure(&cfg.Logging, true)
	log.Infof("Logging Level: %v", cfg.Logging.Level)

	if host == "" {
		ip, err := tchannel.ListenIP()
		if err != nil {
			log.WithField(common.TagErr, err).Fatal(`Failed to find local listen IP`)
		}

		host = ip.String()
	}

	instanceName, e := os.Hostname()
	if e != nil {
		log.WithField(common.TagErr, e).Fatal(`Error getting hostname`)
	}
	instanceName = strings.Replace(instanceName, "-", ".", -1)

	var reporter common.Reporter
	if emitMetric == "m3" {
		log.Infof("M3 metric reporter: hostport=%v, service=%v", cfg.Metrics.M3.HostPort, cfg.Metrics.M3.Service)
		m, e := cfg.Metrics.New()
		if e != nil {
			log.WithField(common.TagErr, e).Fatal(`Failed to initialize metrics`)
		}

		// create the common tags to be used by all services
		reporter = common.NewM3Reporter(m, map[string]string{
			common.TagHostname: instanceName,
		})
	} else {
		reporter = common.NewSimpleReporter(nil, nil)
	}

	m3ReporterClient := wmetrics.NewClient(reporter, wmetrics.Workflow)

	var engine workflow.Engine

	if !runOnMinionsProduction {
		if host == "127.0.0.1" {
			testBase := workflow.TestBase{}
			options := workflow.TestBaseOptions{}
			options.ClusterHost = host
			options.DropKeySpace = true
			testBase.SetupWorkflowStoreWithOptions(options.TestBaseOptions)
			engine = workflow.NewWorkflowEngine(testBase.WorkflowMgr, testBase.TaskMgr, log.WithField("host", "workflow_host"))
		} else {
			executionPersistence, err2 := persistence.NewCassandraWorkflowExecutionPersistence(host, "workflow")
			if err2 != nil {
				panic(err2)
			}

			executionPersistenceClient := workflow.NewWorkflowExecutionPersistenceClient(executionPersistence, m3ReporterClient)

			taskPersistence, err3 := persistence.NewCassandraTaskPersistence(host, "workflow")
			if err3 != nil {
				panic(err3)
			}

			taskPersistenceClient := workflow.NewTaskPersistenceClient(taskPersistence, m3ReporterClient)

			engine = workflow.NewEngineWithMetricsImpl(
				workflow.NewWorkflowEngine(executionPersistenceClient, taskPersistenceClient, log.WithField("host", "workflow_host")),
				m3ReporterClient)
		}
		h := s.NewStressHost(engine, instanceName, cfg, reporter, false /* runOnMinionsProduction */)
		h.Start()
	} else {
		// Running against production.
		h := s.NewStressHost(nil, instanceName, cfg, reporter, runOnMinionsProduction)
		h.Start()
	}
}

func generateRandomKeyspace(n int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("workflow")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
