package main

import (
	"code.uber.internal/devexp/minions/config"

	"code.uber.internal/go-common.git/x/jaeger"
	"code.uber.internal/go-common.git/x/log"
)

func main() {
	// Load configuration
	appConfig, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error initializing workflow configuration: %s", err)
		return
	}

	metrics, err := appConfig.Metrics.New()
	if err != nil {
		log.Fatalf("Could not connect to metrics: %v", err)
	}
	metrics.Counter("boot").Inc(1)

	// Launch Jaeger tracing.
	closer, err := xjaeger.InitGlobalTracer(appConfig.Jaeger, appConfig.ServiceName, metrics)
	if err != nil {
		log.Fatalf("Jaeger.InitGlobalTracer failed: %v", err)
	}
	defer closer.Close()

	// ...

	// Block forever.
	select {}
}
