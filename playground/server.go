package playground

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/config"
	logger "go.temporal.io/server/common/log"
	"go.temporal.io/server/temporal"
	"google.golang.org/protobuf/types/known/durationpb"
)

func startServer() func() {
	ready := make(chan bool)
	stop := make(chan bool)
	stopped := make(chan bool)

	go func() {
		serverDir := filepath.Join(".")
		serverDir, _ = filepath.Abs(serverDir)
		cfg, err := config.LoadConfig("development-sqlite", filepath.Join(serverDir, "config"), "")
		if err != nil {
			panic(err)
		}
		cfg.DynamicConfigClient.Filepath = filepath.Join(serverDir, cfg.DynamicConfigClient.Filepath)

		os.Setenv("OTEL_BSP_SCHEDULE_DELAY", "0")
		os.Setenv("OTEL_TRACES_EXPORTER", "otlp")
		os.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317")

		l := &toggleLogger{inner: logger.NewZapLogger(logger.BuildZapLogger(logger.Config{
			Format: "console",
			Level:  "ERROR",
		}))}

		s, err := temporal.NewServer(
			temporal.ForServices(temporal.DefaultServices),
			temporal.WithConfig(cfg),
			temporal.WithLogger(l),
		)
		if err != nil {
			panic(err)
		}

		defer func() {
			fmt.Println("[[ SERVER STOPPING ]]")
			l.Disable()                 // the shutdown is *very* noisy
			time.Sleep(5 * time.Second) // leave time to flush OTEL spans
			s.Stop()
			stopped <- true
		}()
		err = s.Start()
		if err != nil {
			panic(err)
		}

		ready <- true
		<-stop
	}()

	<-ready
	fmt.Println("[[ SERVER STARTED ]]")

	// create default namespace
	nsClient, err := client.NewNamespaceClient(client.Options{})
	if err != nil {
		panic(err)
	}
	err = nsClient.Register(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "default",
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("[[ NAMESPACE CREATED ]]")

	return func() {
		stop <- true
		<-stopped
	}
}
