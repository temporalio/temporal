//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination factory_mock.go

package sdk

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	ClientFactory interface {
		// options must include Namespace and should not include: HostPort, ConnectionOptions,
		// MetricsHandler, or Logger (they will be overwritten)
		NewClient(options sdkclient.Options) sdkclient.Client
		GetSystemClient() sdkclient.Client
		NewWorker(client sdkclient.Client, taskQueue string, options sdkworker.Options) sdkworker.Worker
	}

	clientFactory struct {
		hostPort        string
		tlsConfig       *tls.Config
		metricsHandler  *MetricsHandler
		logger          log.Logger
		sdklogger       sdklog.Logger
		systemSdkClient sdkclient.Client
		stickyCacheSize dynamicconfig.IntPropertyFn
		once            sync.Once

		// Clients from NewClient share the system client's ref-counted gRPC
		// connection, which the SDK closes only once every one of them is closed,
		// so the factory tracks those still open.
		clientsLock    sync.Mutex
		derivedClients map[*trackedClient]struct{}
		closed         bool
	}

	// trackedClient lets the factory release a client its caller left open.
	trackedClient struct {
		sdkclient.Client
		factory *clientFactory
	}
)

var (
	_ ClientFactory = (*clientFactory)(nil)
)

func NewClientFactory(
	hostPort string,
	tlsConfig *tls.Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
	stickyCacheSize dynamicconfig.IntPropertyFn,
) *clientFactory {
	return &clientFactory{
		hostPort:        hostPort,
		tlsConfig:       tlsConfig,
		metricsHandler:  NewMetricsHandler(metricsHandler),
		logger:          logger,
		sdklogger:       log.NewSdkLogger(logger),
		stickyCacheSize: stickyCacheSize,
		derivedClients:  make(map[*trackedClient]struct{}),
	}
}

func (f *clientFactory) options(options sdkclient.Options) sdkclient.Options {
	options.HostPort = f.hostPort
	options.MetricsHandler = f.metricsHandler
	options.Logger = f.sdklogger
	options.ConnectionOptions = sdkclient.ConnectionOptions{
		TLS: f.tlsConfig,
		DialOptions: []grpc.DialOption{
			grpc.WithUnaryInterceptor(sdkClientNameHeadersInjectorInterceptor()),
		},
	}
	return options
}

func (f *clientFactory) NewClient(options sdkclient.Options) sdkclient.Client {
	clientOptions := f.options(options)

	// Skips the system-client bootstrap for a factory that is already shut down;
	// the check under clientsLock below covers a Close that lands after this one.
	if f.isClosed() {
		return f.newClosedClient(clientOptions)
	}

	system := f.GetSystemClient()

	// NewClientFromExisting takes a reference to the shared connection, so it
	// must not interleave with Close releasing those references.
	f.clientsLock.Lock()
	defer f.clientsLock.Unlock()

	if f.closed {
		return f.newClosedClient(clientOptions)
	}

	// this shouldn't fail if the first client was created successfully
	client, err := sdkclient.NewClientFromExisting(system, clientOptions)
	if err != nil {
		f.logger.Fatal("error creating sdk client", tag.Error(err))
		return nil
	}

	tracked := &trackedClient{Client: client, factory: f}
	f.derivedClients[tracked] = struct{}{}
	return tracked
}

// Called with and without clientsLock held, so it must not take it. Deriving
// from the system client would fetch capabilities over its closed connection, so
// this dials lazily and releases it here, since nothing else would. The wrapper
// is left out of derivedClients so the caller's Close releases nothing rather
// than closing this client a second time.
func (f *clientFactory) newClosedClient(options sdkclient.Options) sdkclient.Client {
	client, err := sdkclient.NewLazyClient(options)
	if err != nil {
		f.logger.Fatal("error creating sdk client", tag.Error(err))
		return nil
	}
	client.Close()
	return &trackedClient{Client: client, factory: f}
}

func (c *trackedClient) Close() {
	// Removing c from the factory carries the right to close it, so the factory
	// and the caller never close the same client concurrently. The SDK's own
	// guard against repeated Close is only safe sequentially.
	if c.factory.release(c) {
		c.Client.Close()
	}
}

func (f *clientFactory) release(c *trackedClient) bool {
	f.clientsLock.Lock()
	defer f.clientsLock.Unlock()

	_, held := f.derivedClients[c]
	delete(f.derivedClients, c)
	return held
}

func (f *clientFactory) GetSystemClient() sdkclient.Client {
	f.once.Do(func() {
		options := f.options(sdkclient.Options{Namespace: primitives.SystemLocalNamespace})

		var sdkClient sdkclient.Client
		err := backoff.ThrottleRetry(func() error {
			var err error

			// Checked on every attempt because this retry cannot be cancelled: a
			// Close landing mid-dial would otherwise keep reaching for a frontend
			// that is going away until the policy expires, and then abort the
			// process partway through shutdown. A lazy client skips the
			// capability fetch, so it fails fast instead.
			if f.isClosed() {
				sdkClient, err = sdkclient.NewLazyClient(options)
				return err
			}

			sdkClient, err = sdkclient.Dial(options)
			if err != nil {
				f.logger.Warn("error creating sdk client", tag.Error(err))
			}
			return err
		}, common.CreateSdkClientFactoryRetryPolicy(), func(err error) bool {
			// note err is wrapped by sdk
			var unavail *serviceerror.Unavailable
			return common.IsContextDeadlineExceededErr(err) || errors.As(err, &unavail)
		})
		if err != nil {
			f.logger.Fatal("error creating sdk client", tag.Error(err))
			return
		}
		f.setSystemClient(sdkClient)

		if size := f.stickyCacheSize(); size > 0 {
			f.logger.Info("setting sticky workflow cache size", tag.Int("size", size))
			sdkworker.SetStickyWorkflowCacheSize(size)
		}
	})
	return f.systemSdkClient
}

func (f *clientFactory) isClosed() bool {
	f.clientsLock.Lock()
	defer f.clientsLock.Unlock()

	return f.closed
}

// setSystemClient publishes the shared client, closing it if the factory shut
// down while it was being dialed, since Close could not have seen it.
func (f *clientFactory) setSystemClient(client sdkclient.Client) {
	f.clientsLock.Lock()
	f.systemSdkClient = client
	closed := f.closed
	f.clientsLock.Unlock()

	if closed && client != nil {
		client.Close()
	}
}

func (f *clientFactory) NewWorker(
	client sdkclient.Client,
	taskQueue string,
	options sdkworker.Options,
) sdkworker.Worker {
	// sdkworker.New type-asserts the SDK's concrete client, so unwrap.
	if tracked, ok := client.(*trackedClient); ok {
		client = tracked.Client
	}
	return sdkworker.New(client, taskQueue, options)
}

// Close releases the shared system SDK client and the gRPC connection backing
// it, along with the clients derived from it by NewClient. It is safe to call
// more than once.
func (f *clientFactory) Close() {
	f.clientsLock.Lock()
	defer f.clientsLock.Unlock()

	// Close is idempotent: every client is released exactly once.
	if f.closed {
		return
	}
	f.closed = true

	// Release the references callers left open before the system client, so the
	// SDK's reference count reaches zero and the shared connection is closed.
	for client := range f.derivedClients {
		client.Client.Close()
	}
	clear(f.derivedClients)

	// The field stays set so GetSystemClient never returns nil.
	if f.systemSdkClient != nil {
		f.systemSdkClient.Close()
	}
}

// Overwrite the 'client-name' and 'client-version' headers on gRPC requests sent using the Go SDK
// so they clearly indicate that the request is coming from the Temporal server.
func sdkClientNameHeadersInjectorInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// Can't use headers.SetVersions() here because it is _appending_ headers to the context
		// rather than _replacing_ them, which means Go SDK's default headers would still be present.
		md, mdExist := metadata.FromOutgoingContext(ctx)
		if !mdExist {
			md = metadata.New(nil)
		}
		md.Set(headers.ClientNameHeaderName, headers.ClientNameServer)
		md.Set(headers.ClientVersionHeaderName, headers.ServerVersion)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
