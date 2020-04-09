package cli

import (
	"github.com/urfave/cli"
	"go.temporal.io/temporal-proto/workflowservice"
	sdkclient "go.temporal.io/temporal/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/common/rpc"
)

// ClientFactory is used to construct rpc clients
type ClientFactory interface {
	FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient
	AdminClient(c *cli.Context) adminservice.AdminServiceClient
	SDKClient(c *cli.Context, namespace string) sdkclient.Client
}

type clientFactory struct {
	logger *zap.Logger
}

// NewClientFactory creates a new ClientFactory
func NewClientFactory() ClientFactory {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return &clientFactory{
		logger: logger,
	}
}

// FrontendClient builds a frontend client
func (b *clientFactory) FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient {
	connection := b.createGRPCConnection(c.GlobalString(FlagAddress))

	return workflowservice.NewWorkflowServiceClient(connection)
}

// AdminClient builds an admin client (based on server side thrift interface)
func (b *clientFactory) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	connection := b.createGRPCConnection(c.GlobalString(FlagAddress))

	return adminservice.NewAdminServiceClient(connection)
}

// AdminClient builds an admin client (based on server side thrift interface)
func (b *clientFactory) SDKClient(c *cli.Context, namespace string) sdkclient.Client {
	hostPort := c.GlobalString(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}

	sdkClient, err := sdkclient.NewClient(sdkclient.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
	if err != nil {
		b.logger.Fatal("Failed to create SDK client", zap.Error(err))
	}

	return sdkClient
}

func (b *clientFactory) createGRPCConnection(hostPort string) *grpc.ClientConn {
	if hostPort == "" {
		hostPort = localHostPort
	}

	connection, err := rpc.Dial(hostPort)
	if err != nil {
		b.logger.Fatal("Failed to create connection", zap.Error(err))
		return nil
	}

	return connection
}
