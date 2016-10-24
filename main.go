package main

import (
	gen "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/jaeger"
	"code.uber.internal/go-common.git/x/log"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

func main() {
	var cfg appConfig
	if err := config.Load(&cfg); err != nil {
		log.Fatalf("Error initializing configuration: %s", err)
	}
	log.Configure(&cfg.Logging, cfg.Verbose)
	log.ConfigureSentry(&cfg.Sentry)

	metrics, err := cfg.Metrics.New()
	if err != nil {
		log.Fatalf("Could not connect to metrics: %v", err)
	}
	metrics.Counter("boot").Inc(1)

	closer, err := xjaeger.InitGlobalTracer(cfg.Jaeger, cfg.ServiceName, metrics)
	if err != nil {
		log.Fatalf("Jaeger.InitGlobalTracer failed: %v", err)
	}
	defer closer.Close()

	if _, err := cfg.TChannel.New(cfg.ServiceName, metrics, registerHandlers); err != nil {
		log.Fatalf("TChannel.New failed: %v", err)
	}

	// If the server needs to listen on HTTP port, use dynamic port:
	// port, _ := config.GetDynamicHTTPPort(cfg.Port)

	// Block forever.
	select {}
}

func registerHandlers(ch *tchannel.Channel, server *thrift.Server) {
	server.Register(gen.NewTChanMyServiceServer(myHandler{}))
}

type myHandler struct{}

func (myHandler) Hello(ctx thrift.Context) (string, error) {
	return "Hello World!", nil
}
