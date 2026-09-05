package localexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	BridgeBootstrapPath       = "/bootstrap"
	bridgeBootstrapBodyLimit  = 4 << 20
	minimumBootstrapTokenSize = 32
)

type BridgeConfiguration struct {
	Namespace     string                     `json:"namespace"`
	Upstream      UpstreamConnectionProfile  `json:"upstream"`
	Options       BridgeLocalFirstOptions    `json:"options"`
	Registrations WorkerRegistrationManifest `json:"registrations"`
}

type UpstreamConnectionProfile struct {
	Address    string            `json:"address"`
	Identity   string            `json:"identity,omitempty"`
	ServerName string            `json:"server_name,omitempty"`
	TLS        *UpstreamTLS      `json:"tls,omitempty"`
	APIKey     string            `json:"api_key,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

type UpstreamTLS struct {
	ServerRootCACertificate string `json:"server_root_ca_certificate,omitempty"`
	ClientCertificate       string `json:"client_certificate,omitempty"`
	ClientPrivateKey        string `json:"client_private_key,omitempty"`
}

type BridgeLocalFirstOptions struct {
	SyncIntervalMilliseconds    int64 `json:"sync_interval_milliseconds"`
	MaximumUnsynchronizedEvents int64 `json:"maximum_unsynchronized_events"`
	MaximumUnsynchronizedBytes  int64 `json:"maximum_unsynchronized_bytes"`
}

type WorkerRegistrationManifest struct {
	TaskQueue     string   `json:"task_queue"`
	WorkflowTypes []string `json:"workflow_types,omitempty"`
	ActivityTypes []string `json:"activity_types,omitempty"`
}

type BridgeBootstrapResponse struct {
	FrontendAddress string `json:"frontend_address"`
	LocalServerID   string `json:"local_server_id"`
}

type BridgeBootstrapHandler func(
	ctx context.Context,
	configuration BridgeConfiguration,
) (BridgeBootstrapResponse, error)

type BridgeBootstrapServer struct {
	listener net.Listener
	handler  BridgeBootstrapHandler
	server   *http.Server

	mu            sync.Mutex
	tokenHash     [sha256.Size]byte
	bootstrapping bool
}

func NewBridgeBootstrapServer(
	listener net.Listener,
	token []byte,
	handler BridgeBootstrapHandler,
) (*BridgeBootstrapServer, error) {
	if listener == nil {
		return nil, errors.New("bootstrap listener is required")
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !tcpAddress.IP.IsLoopback() {
		return nil, errors.New("bootstrap listener must use a loopback TCP address")
	}
	if len(token) < minimumBootstrapTokenSize {
		return nil, fmt.Errorf("bootstrap token must contain at least %d bytes", minimumBootstrapTokenSize)
	}
	if handler == nil {
		return nil, errors.New("bootstrap handler is required")
	}

	bootstrapServer := &BridgeBootstrapServer{
		listener:  listener,
		handler:   handler,
		tokenHash: sha256.Sum256(token),
	}
	for index := range token {
		token[index] = 0
	}
	mux := http.NewServeMux()
	mux.HandleFunc(BridgeBootstrapPath, bootstrapServer.handleBootstrap)
	bootstrapServer.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return bootstrapServer, nil
}

func ReadAndRemoveBootstrapToken(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("bootstrap token file is required")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect bootstrap token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("bootstrap token file must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("bootstrap token file must not be accessible by group or other users")
	}
	if info.Size() > 4096 {
		return nil, errors.New("bootstrap token file is too large")
	}
	token, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap token file: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("remove bootstrap token file: %w", err)
	}
	token = bytes.TrimSpace(token)
	if len(token) < minimumBootstrapTokenSize {
		return nil, fmt.Errorf("bootstrap token must contain at least %d bytes", minimumBootstrapTokenSize)
	}
	return token, nil
}

func (s *BridgeBootstrapServer) Address() string {
	return "http://" + s.listener.Addr().String()
}

func (s *BridgeBootstrapServer) Serve() error {
	err := s.server.Serve(s.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *BridgeBootstrapServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *BridgeBootstrapServer) handleBootstrap(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.authenticate(request.Header.Get("Authorization")) {
		writer.WriteHeader(http.StatusUnauthorized)
		return
	}

	s.mu.Lock()
	if s.bootstrapping {
		s.mu.Unlock()
		writer.WriteHeader(http.StatusConflict)
		return
	}
	s.bootstrapping = true
	s.mu.Unlock()

	succeeded := false
	defer func() {
		s.mu.Lock()
		if succeeded {
			s.tokenHash = [sha256.Size]byte{}
		} else {
			s.bootstrapping = false
		}
		s.mu.Unlock()
	}()

	configuration, err := decodeBridgeConfiguration(writer, request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if err := configuration.Validate(); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	response, err := s.handler(request.Context(), configuration)
	if err != nil {
		http.Error(writer, "bridge startup failed", http.StatusServiceUnavailable)
		return
	}
	succeeded = true
	if err := validateBridgeBootstrapResponse(response); err != nil {
		http.Error(writer, "bridge startup returned an invalid response", http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		return
	}
}

func (s *BridgeBootstrapServer) authenticate(authorization string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return false
	}
	presentedHash := sha256.Sum256([]byte(strings.TrimPrefix(authorization, prefix)))
	s.mu.Lock()
	defer s.mu.Unlock()
	return subtle.ConstantTimeCompare(presentedHash[:], s.tokenHash[:]) == 1
}

func decodeBridgeConfiguration(
	writer http.ResponseWriter,
	request *http.Request,
) (BridgeConfiguration, error) {
	request.Body = http.MaxBytesReader(writer, request.Body, bridgeBootstrapBodyLimit)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var configuration BridgeConfiguration
	if err := decoder.Decode(&configuration); err != nil {
		return BridgeConfiguration{}, fmt.Errorf("decode bridge configuration: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return BridgeConfiguration{}, errors.New("bridge configuration must contain one JSON value")
	}
	return configuration, nil
}

func (c BridgeConfiguration) Validate() error {
	if c.Namespace == "" {
		return errors.New("namespace is required")
	}
	if c.Upstream.Address == "" {
		return errors.New("upstream address is required")
	}
	if c.Options.SyncIntervalMilliseconds <= 0 {
		return errors.New("sync interval must be positive")
	}
	if c.Options.MaximumUnsynchronizedEvents <= 0 {
		return errors.New("maximum unsynchronized events must be positive")
	}
	if c.Options.MaximumUnsynchronizedBytes <= 0 {
		return errors.New("maximum unsynchronized bytes must be positive")
	}
	if c.Registrations.TaskQueue == "" {
		return errors.New("registration task queue is required")
	}
	if err := validateRegistrationTypes("workflow", c.Registrations.WorkflowTypes); err != nil {
		return err
	}
	if err := validateRegistrationTypes("activity", c.Registrations.ActivityTypes); err != nil {
		return err
	}
	if tls := c.Upstream.TLS; tls != nil &&
		((tls.ClientCertificate == "") != (tls.ClientPrivateKey == "")) {
		return errors.New("upstream TLS client certificate and private key must be provided together")
	}
	for name, value := range c.Upstream.Headers {
		if name == "" || strings.ContainsAny(name+value, "\r\n") {
			return errors.New("upstream headers must have non-empty names and contain no newlines")
		}
	}
	return nil
}

func validateRegistrationTypes(kind string, types []string) error {
	seen := make(map[string]struct{}, len(types))
	for _, registeredType := range types {
		if registeredType == "" {
			return fmt.Errorf("registered %s type must not be empty", kind)
		}
		if _, exists := seen[registeredType]; exists {
			return fmt.Errorf("registered %s type %q is duplicated", kind, registeredType)
		}
		seen[registeredType] = struct{}{}
	}
	return nil
}

func validateBridgeBootstrapResponse(response BridgeBootstrapResponse) error {
	if response.FrontendAddress == "" {
		return errors.New("frontend address is required")
	}
	if response.LocalServerID == "" {
		return errors.New("local server ID is required")
	}
	return nil
}
