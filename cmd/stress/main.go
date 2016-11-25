package main

import (
	"flag"
	"os"
	"strings"

	"github.com/uber/tchannel-go"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/devexp/minions/common"
	s "code.uber.internal/devexp/minions/health/stress"
)

func main() {
	var host string
	var emitMetric string

	flag.StringVar(&host, "host", "", "Cassandra host to use.")
	flag.StringVar(&emitMetric, "emitMetric", "local", "Metric source: m3 | local")

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
		m, e := cfg.StressConfig.Metrics.New()
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

	h := s.NewStressHost(host, instanceName, cfg, reporter)
	h.Start()
}
