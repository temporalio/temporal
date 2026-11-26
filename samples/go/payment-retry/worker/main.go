package main

import (
	"log"
	// "net/http" // Not needed as Tally reporter will expose metrics endpoint
	"samples/go/payment-retry/workflow" // Your workflow package
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/worker"

	"github.com/uber-go/tally/v4/prometheus"
	// promhttp "github.com/prometheus/client_golang/prometheus/promhttp" // Not needed
)

const PaymentTaskQueue = "PAYMENT_TASK_QUEUE"

func main() {
	// Configure Tally Prometheus reporter
	// Tally will serve metrics on ListenAddress (e.g., http://localhost:9090/metrics)
	reporterOpts := prometheus.Configuration{
		ListenAddress: "0.0.0.0:9090",
		TimerType:     "histogram",
	}
	// This reporter will start an HTTP server if ListenAddress is configured.
	reporter := prometheus.NewReporter(reporterOpts)

	scopeOpts := tally.ScopeOptions{
		Separator:       "_", // Use "_" to be more Prometheus idiomatic
		SanitizeOptions: &contribtally.PrometheusSanitizeOptions, // Ensure metric names are Prometheus-compatible
		Reporter:        reporter,
		// Tags: map[string]string{}, // Optional global tags for all metrics
	}
	// Create a root scope for metrics, reporting every second
	rootScope, closer := tally.NewRootScope(scopeOpts, 1*time.Second)
	defer closer.Close() // Ensure the reporter is closed gracefully

	// Create a Temporal metrics handler from the Tally scope
	metricsHandler := contribtally.NewMetricsHandler(rootScope)

	// Create Temporal client
	c, err := client.Dial(client.Options{
		HostPort:       client.DefaultHostPort, // Default: localhost:7233
		MetricsHandler: metricsHandler,
		// Namespace: client.DefaultNamespace, // Default: "default"
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client", "error", err)
	}
	defer c.Close()

	// Create Worker
	// worker.Options can be used to configure concurrency, rate limits, etc.
	w := worker.New(c, PaymentTaskQueue, worker.Options{})

	// Register Workflow and Activity
	w.RegisterWorkflow(workflow.PaymentWorkflow)

	// Instantiate the activity. If PaymentActivity has no fields,
	// workflow.PaymentActivity{} is fine. If it has dependencies or state
	// that needs to be configured, initialize them here.
	activityInstance := &workflow.PaymentActivity{}
	w.RegisterActivity(activityInstance)

	log.Println("Starting Worker. Task Queue:", PaymentTaskQueue)
	log.Println("Temporal Client Namespace:", client.DefaultNamespace) // Or the configured namespace
	log.Println("Temporal Host:Port:", client.DefaultHostPort)      // Or the configured host:port
	log.Println("Metrics will be available at http://localhost:9090/metrics")

	// Start Worker. This call blocks until the worker is stopped or an error occurs.
	// worker.InterruptCh() listens for termination signals (like SIGINT, SIGTERM)
	// to gracefully shut down the worker.
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start Worker", "error", err)
	}
}
