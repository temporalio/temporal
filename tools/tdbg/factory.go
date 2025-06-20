package tdbg

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// HttpGetter defines http.Client.Get(...) as an interface so we can mock it
	HttpGetter interface {
		Get(url string) (resp *http.Response, err error)
	}
	// ClientFactory is used to construct rpc clients
	ClientFactory interface {
		AdminClient(c *cli.Context) adminservice.AdminServiceClient
		WorkflowClient(c *cli.Context) workflowservice.WorkflowServiceClient
	}
	// ClientFactoryOption is used to configure the ClientFactory via NewClientFactory.
	ClientFactoryOption func(params *clientFactoryParams)
	// DefaultFrontendAddressProvider uses FlagAddress to determine the frontend address, defaulting to
	// DefaultFrontendAddress if FlagAddress is not set or is empty.
	DefaultFrontendAddressProvider struct{}

	clientFactory struct {
		logger                  log.Logger
		frontendAddressProvider frontendAddressProvider
	}
	clientFactoryParams struct {
		frontendAddressProvider frontendAddressProvider
	}
	frontendAddressProvider interface {
		GetFrontendAddress(c *cli.Context) string
	}
	staticFrontendAddress string
)

var netClient HttpGetter = &http.Client{
	Timeout: time.Second * 10,
}

// NewClientFactory creates a new ClientFactory
func NewClientFactory(opts ...ClientFactoryOption) ClientFactory {
	logger := log.NewCLILogger()
	params := &clientFactoryParams{
		frontendAddressProvider: DefaultFrontendAddressProvider{},
	}
	for _, opt := range opts {
		opt(params)
	}

	return &clientFactory{
		logger:                  logger,
		frontendAddressProvider: params.frontendAddressProvider,
	}
}

// WithFrontendAddress ensures that admin clients created by the factory will connect to the specified address.
func WithFrontendAddress(address string) ClientFactoryOption {
	return func(params *clientFactoryParams) {
		params.frontendAddressProvider = staticFrontendAddress(address)
	}
}

// AdminClient builds an admin client.
func (b *clientFactory) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return adminservice.NewAdminServiceClient(connection)
}

func (b *clientFactory) WorkflowClient(c *cli.Context) workflowservice.WorkflowServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return workflowservice.NewWorkflowServiceClient(connection)
}

func (b *clientFactory) createGRPCConnection(c *cli.Context) (*grpc.ClientConn, error) {
	frontendAddress := b.frontendAddressProvider.GetFrontendAddress(c)

	tlsConfig, err := b.createTLSConfig(c)
	if err != nil {
		return nil, err
	}

	grpcSecurityOptions := grpc.WithTransportCredentials(insecure.NewCredentials())

	if tlsConfig != nil {
		grpcSecurityOptions = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	dialOpts := []grpc.DialOption{
		grpcSecurityOptions,
	}

	connection, err := grpc.NewClient(frontendAddress, dialOpts...)
	if err != nil {
		b.logger.Fatal("Failed to create connection", tag.Error(err))
		return nil, err
	}
	return connection, nil
}

func (b *clientFactory) createTLSConfig(c *cli.Context) (*tls.Config, error) {
	certPath := c.String(FlagTLSCertPath)
	keyPath := c.String(FlagTLSKeyPath)
	caPath := c.String(FlagTLSCaPath)
	disableHostNameVerificationS := c.String(FlagTLSDisableHostVerification)
	disableHostNameVerification, err := strconv.ParseBool(disableHostNameVerificationS)
	if err != nil {
		return nil, fmt.Errorf("unable to read TLS disable host verification flag: %s", err)
	}

	serverName := c.String(FlagTLSServerName)

	var host string
	var cert *tls.Certificate
	var caPool *x509.CertPool

	if caPath != "" {
		caCertPool, err := fetchCACert(caPath)
		if err != nil {
			b.logger.Fatal("Failed to load server CA certificate", tag.Error(err))
			return nil, err
		}
		caPool = caCertPool
	}
	if certPath != "" {
		myCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			b.logger.Fatal("Failed to load client certificate", tag.Error(err))
			return nil, err
		}
		cert = &myCert
	}
	// If we are given arguments to verify either server or client, configure TLS
	if caPool != nil || cert != nil {
		if serverName != "" {
			host = serverName
		} else {
			hostPort := c.String(FlagAddress)
			if hostPort == "" {
				hostPort = DefaultFrontendAddress
			}
			// Ignoring error as we'll fail to dial anyway, and that will produce a meaningful error
			host, _, _ = net.SplitHostPort(hostPort)
		}
		tlsConfig := auth.NewTLSConfigForServer(host, !disableHostNameVerification)
		if caPool != nil {
			tlsConfig.RootCAs = caPool
		}
		if cert != nil {
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}

		return tlsConfig, nil
	}
	// If we are given a server name, set the TLS server name for DNS resolution
	if serverName != "" {
		host = serverName
		tlsConfig := auth.NewTLSConfigForServer(host, !disableHostNameVerification)
		return tlsConfig, nil
	}

	return nil, nil
}

func fetchCACert(pathOrUrl string) (caPool *x509.CertPool, err error) {
	caPool = x509.NewCertPool()
	var caBytes []byte

	if strings.HasPrefix(pathOrUrl, "http://") {
		return nil, errors.New("HTTP is not supported for CA cert URLs. Provide HTTPS URL")
	}

	if strings.HasPrefix(pathOrUrl, "https://") {
		var resp *http.Response
		resp, err = netClient.Get(pathOrUrl)
		if err != nil {
			return nil, err
		}
		defer func() {
			// see https://pkg.go.dev/go.uber.org/multierr#hdr-Deferred_Functions
			err = multierr.Combine(err, resp.Body.Close())
		}()
		caBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
	} else {
		caBytes, err = os.ReadFile(pathOrUrl)
		if err != nil {
			return nil, err
		}
	}

	if !caPool.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("unknown failure constructing cert pool for ca")
	}
	return caPool, nil
}

func (address staticFrontendAddress) GetFrontendAddress(*cli.Context) string {
	return string(address)
}

func (d DefaultFrontendAddressProvider) GetFrontendAddress(c *cli.Context) string {
	if addr := c.String(FlagAddress); addr != "" {
		return addr
	}
	return DefaultFrontendAddress
}
