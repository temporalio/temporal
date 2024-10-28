// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package frontend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/inline"
	"go.temporal.io/server/common/utf8validator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// HTTPAPIServer is an HTTP API server that forwards requests to gRPC via the
// gRPC interceptors.
type HTTPAPIServer struct {
	server                        http.Server
	listener                      net.Listener
	logger                        log.Logger
	serveMux                      *runtime.ServeMux
	stopped                       chan struct{}
	matchAdditionalHeaders        map[string]bool
	matchAdditionalHeaderPrefixes []string
}

var defaultForwardedHeaders = []string{
	"Authorization-Extras",
	"X-Forwarded-For",
	http.CanonicalHeaderKey(headers.ClientNameHeaderName),
	http.CanonicalHeaderKey(headers.ClientVersionHeaderName),
}

type httpRemoteAddrContextKey struct{}

var (
	errHTTPGRPCListenerNotTCP = errors.New("must use TCP for gRPC listener to support HTTP API")
)

// NewHTTPAPIServer creates an [HTTPAPIServer].
//
// routes registered with additionalRouteRegistrationFuncs take precedence over the auto generated grpc proxy routes.
func NewHTTPAPIServer(
	serviceConfig *Config,
	rpcConfig config.RPC,
	grpcListener net.Listener,
	tlsConfigProvider encryption.TLSConfigProvider,
	handler Handler,
	operatorHandler *OperatorHandlerImpl,
	serverInterceptors []grpc.UnaryServerInterceptor,
	metricsHandler metrics.Handler,
	router *mux.Router,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) (*HTTPAPIServer, error) {
	// Create a TCP listener the same as the frontend one but with different port
	tcpAddrRef, _ := grpcListener.Addr().(*net.TCPAddr)
	if tcpAddrRef == nil {
		return nil, errHTTPGRPCListenerNotTCP
	}
	tcpAddr := *tcpAddrRef
	tcpAddr.Port = rpcConfig.HTTPPort
	var listener net.Listener
	var err error
	if listener, err = net.ListenTCP("tcp", &tcpAddr); err != nil {
		return nil, fmt.Errorf("failed listening for HTTP API on %v: %w", &tcpAddr, err)
	}
	// Close the listener if anything else in this function fails
	success := false
	defer func() {
		if !success {
			_ = listener.Close()
		}
	}()

	// Wrap the listener in a TLS listener if there is any TLS config
	if tlsConfigProvider != nil {
		if tlsConfig, err := tlsConfigProvider.GetFrontendServerConfig(); err != nil {
			return nil, fmt.Errorf("failed getting TLS config for HTTP API: %w", err)
		} else if tlsConfig != nil {
			listener = tls.NewListener(listener, tlsConfig)
		}
	}

	h := &HTTPAPIServer{
		listener: listener,
		logger:   logger,
		stopped:  make(chan struct{}),
	}

	// Build 4 possible marshalers in order based on content type
	opts := []runtime.ServeMuxOption{
		runtime.WithMarshalerOption(newTemporalProtoMarshaler("  ", false)),
		runtime.WithMarshalerOption(newTemporalProtoMarshaler("", false)),
		runtime.WithMarshalerOption(newTemporalProtoMarshaler("  ", true)),
		runtime.WithMarshalerOption(newTemporalProtoMarshaler("", true)),
	}

	// Set Temporal service error handler
	opts = append(opts, runtime.WithErrorHandler(h.errorHandler))

	// Match headers w/ default
	h.matchAdditionalHeaders = map[string]bool{}
	for _, v := range defaultForwardedHeaders {
		h.matchAdditionalHeaders[v] = true
	}
	for _, v := range rpcConfig.HTTPAdditionalForwardedHeaders {
		if strings.HasSuffix(v, "*") {
			h.matchAdditionalHeaderPrefixes = append(h.matchAdditionalHeaderPrefixes, http.CanonicalHeaderKey(strings.TrimSuffix(v, "*")))
		} else {
			h.matchAdditionalHeaders[http.CanonicalHeaderKey(v)] = true
		}
	}

	opts = append(opts, runtime.WithIncomingHeaderMatcher(h.incomingHeaderMatcher))

	clientInterceptors := []grpc.UnaryClientInterceptor{setDefaultClientHeadersForHTTP}

	// Create inline client connection
	counter := metrics.HTTPServiceRequests.With(metricsHandler)
	clientConn := inline.NewInlineClientConn()
	clientConn.RegisterServer(
		"temporal.api.workflowservice.v1.WorkflowService",
		handler,
		clientInterceptors,
		serverInterceptors,
		counter,
		namespaceRegistry,
	)
	clientConn.RegisterServer(
		"temporal.api.operatorservice.v1.OperatorService",
		operatorHandler,
		clientInterceptors,
		serverInterceptors,
		counter,
		namespaceRegistry,
	)

	// Create serve mux
	h.serveMux = runtime.NewServeMux(opts...)

	err = workflowservice.RegisterWorkflowServiceHandlerClient(
		context.Background(),
		h.serveMux,
		workflowservice.NewWorkflowServiceClient(clientConn),
	)
	if err != nil {
		return nil, fmt.Errorf("failed registering workflowservice HTTP API handler: %w", err)
	}

	err = operatorservice.RegisterOperatorServiceHandlerClient(
		context.Background(),
		h.serveMux,
		operatorservice.NewOperatorServiceClient(clientConn),
	)
	if err != nil {
		return nil, fmt.Errorf("failed registering operatorservice HTTP API handler: %w", err)
	}

	// Set the / handler as our function that wraps serve mux.
	router.PathPrefix("/").HandlerFunc(h.serveHTTP)
	// Register the router as the HTTP server handler.
	h.server.Handler = router

	// Put the remote address on the context
	h.server.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		return context.WithValue(ctx, httpRemoteAddrContextKey{}, c)
	}

	// We want to set ReadTimeout and WriteTimeout as max idle (and IdleTimeout
	// defaults to ReadTimeout) to ensure that a connection cannot hang over that
	// amount of time.
	h.server.ReadTimeout = serviceConfig.KeepAliveMaxConnectionIdle()
	h.server.WriteTimeout = serviceConfig.KeepAliveMaxConnectionIdle()

	success = true
	return h, nil
}

// Serve serves the HTTP API and does not return until there is a serve error or
// GracefulStop completes. Upon graceful stop, this will return nil. If an error
// is returned, the message is clear that it came from the HTTP API server.
func (h *HTTPAPIServer) Serve() error {
	err := h.server.Serve(h.listener)
	// If the error is for close, we have to wait for the shutdown to complete and
	// we don't consider it an error
	if errors.Is(err, http.ErrServerClosed) {
		<-h.stopped
		err = nil
	}
	// Wrap the error to be clearer it's from the HTTP API
	if err != nil {
		return fmt.Errorf("HTTP API serve failed: %w", err)
	}
	return nil
}

// GracefulStop stops the HTTP server. This will first attempt a graceful stop
// with a drain time, then will hard-stop. This will not return until stopped.
func (h *HTTPAPIServer) GracefulStop(gracefulDrainTime time.Duration) {
	// We try a graceful stop for the amount of time we can drain, then we do a
	// hard stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulDrainTime)
	defer cancel()
	// We intentionally ignore this error, we're gonna stop at this point no
	// matter what. This closes the listener too.
	_ = h.server.Shutdown(shutdownCtx)
	_ = h.server.Close()
	close(h.stopped)
}

func (h *HTTPAPIServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// Limit the request body to max gRPC size. This is hardcoded to 4MB at the
	// moment using gRPC's default at
	// https://github.com/grpc/grpc-go/blob/0673105ebcb956e8bf50b96e28209ab7845a65ad/server.go#L58
	// which is what the constant is set as at the time of this comment.
	r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxHTTPAPIRequestBytes)

	h.logger.Debug(
		"HTTP API call",
		tag.NewStringTag("http-method", r.Method),
		tag.NewAnyTag("http-url", r.URL),
	)

	// Need to change the accept header based on whether pretty and/or
	// noPayloadShorthand are present
	var acceptHeaderSuffix string
	if _, ok := r.URL.Query()["pretty"]; ok {
		acceptHeaderSuffix += "+pretty"
	}
	if _, ok := r.URL.Query()["noPayloadShorthand"]; ok {
		acceptHeaderSuffix += "+no-payload-shorthand"
	}
	if acceptHeaderSuffix != "" {
		r.Header.Set("Accept", "application/json"+acceptHeaderSuffix)
	}

	// Put the TLS info on the peer context
	if r.TLS != nil {
		var addr net.Addr
		if conn, _ := r.Context().Value(httpRemoteAddrContextKey{}).(net.Conn); conn != nil {
			addr = conn.RemoteAddr()
		}
		r = r.WithContext(peer.NewContext(r.Context(), &peer.Peer{
			Addr: addr,
			AuthInfo: credentials.TLSInfo{
				State:          *r.TLS,
				CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
			},
		}))
	}

	// Call gRPC gateway mux
	h.serveMux.ServeHTTP(w, r)
}

func (h *HTTPAPIServer) errorHandler(
	ctx context.Context,
	mux *runtime.ServeMux,
	marshaler runtime.Marshaler,
	w http.ResponseWriter,
	r *http.Request,
	err error,
) {
	// Convert the error using serviceerror. The result does not conform to Google
	// gRPC status directly (it conforms to gogo gRPC status), but Err() does
	// based on internal code reading. However, Err() uses Google proto Any
	// which our marshaler is not expecting. So instead we are embedding similar
	// logic to runtime.DefaultHTTPProtoErrorHandler in here but with gogo
	// support. We don't implement custom content type marshaler or trailers at
	// this time.

	s := serviceerror.ToStatus(err)
	w.Header().Set("Content-Type", marshaler.ContentType(struct{}{}))

	sProto := s.Proto()
	var buf []byte
	merr := utf8validator.Validate(sProto, utf8validator.SourceRPCResponse)
	if merr == nil {
		buf, merr = marshaler.Marshal(sProto)
	}
	if merr != nil {
		h.logger.Warn("Failed to marshal error message", tag.Error(merr))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code": 13, "message": "failed to marshal error message"}`))
		return
	}

	w.WriteHeader(runtime.HTTPStatusFromCode(s.Code()))
	_, _ = w.Write(buf)
}

func (h *HTTPAPIServer) incomingHeaderMatcher(headerName string) (string, bool) {
	// Try ours before falling back to default
	if h.matchAdditionalHeaders[headerName] {
		return headerName, true
	}
	for _, prefix := range h.matchAdditionalHeaderPrefixes {
		if strings.HasPrefix(headerName, prefix) {
			return headerName, true
		}
	}
	return runtime.DefaultHeaderMatcher(headerName)
}

func setDefaultClientHeadersForHTTP(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Set the client and version headers if not already set
	md, _ := metadata.FromOutgoingContext(ctx)
	if len(md[headers.ClientNameHeaderName]) == 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, headers.ClientNameHeaderName, headers.ClientNameServerHTTP)
	}
	if len(md[headers.ClientVersionHeaderName]) == 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, headers.ClientVersionHeaderName, headers.ServerVersion)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}
