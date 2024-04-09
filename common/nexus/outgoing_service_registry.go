// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexus

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"regexp"

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
)

// OutgoingServiceRegistry manages the registration and retrieval of "outgoing" services from the namespace metadata in
// the persistence layer. An outgoing service is a Nexus service to which we send traffic. For example, we might call
// one of these services to start an operation using this API: https://github.com/nexus-rpc/api/blob/main/SPEC.md#start-operation.
// We need a registry for these services, so that we can look up the URL for a service by name. Later, we may add more
// service-specific information to the registry.
type OutgoingServiceRegistry struct {
	namespaceService    NamespaceService
	namespaceReplicator namespace.Replicator
	config              *OutgoingServiceRegistryConfig
	sortedSetManager    collection.SortedSetManager[[]*persistencespb.NexusOutgoingService, *persistencespb.NexusOutgoingService, string]
}

// NamespaceService is an interface which contains only the methods we need from
// [go.temporal.io/server/common/persistence.MetadataManager].
type NamespaceService interface {
	GetNamespace(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error)
	UpdateNamespace(ctx context.Context, request *persistence.UpdateNamespaceRequest) error
}

// NewOutgoingServiceRegistry creates a new [OutgoingServiceRegistry] with the given namespace service and configuration.
func NewOutgoingServiceRegistry(
	namespaceService NamespaceService,
	namespaceReplicator namespace.Replicator,
	config *OutgoingServiceRegistryConfig,
) *OutgoingServiceRegistry {
	return &OutgoingServiceRegistry{
		namespaceService:    namespaceService,
		namespaceReplicator: namespaceReplicator,
		config:              config,
		sortedSetManager: collection.NewSortedSetManager[[]*persistencespb.NexusOutgoingService, *persistencespb.NexusOutgoingService, string](
			func(service *persistencespb.NexusOutgoingService, name string) int {
				return bytes.Compare([]byte(service.Name), []byte(name))
			},
			func(service *persistencespb.NexusOutgoingService) string {
				return service.Name
			},
		),
	}
}

// OutgoingServiceRegistryConfig contains the dynamic configuration values for the [OutgoingServiceRegistry].
type OutgoingServiceRegistryConfig struct {
	MaxURLLength    dynamicconfig.IntPropertyFn
	MaxNameLength   dynamicconfig.IntPropertyFn
	DefaultPageSize dynamicconfig.IntPropertyFn
	MaxPageSize     dynamicconfig.IntPropertyFn
}

// NewOutgoingServiceRegistryConfig creates a new [OutgoingServiceRegistryConfig] with the given dynamic configuration.
func NewOutgoingServiceRegistryConfig(dc *dynamicconfig.Collection) *OutgoingServiceRegistryConfig {
	return &OutgoingServiceRegistryConfig{
		MaxURLLength:    dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceURLMaxLength, 1000),
		MaxNameLength:   dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceNameMaxLength, 200),
		DefaultPageSize: dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceListDefaultPageSize, 100),
		MaxPageSize:     dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceListMaxPageSize, 1000),
	}
}

const (
	IssueNamespaceNotSet         = "namespace is not set on request"
	IssueNameNotSet              = "name is not set on request"
	IssueSpecNotSet              = "spec is not set on request"
	IssueURLNotSet               = "url is not set on request"
	IssuePublicCallbackURLNotSet = "public callback url is not set on request"
)

// ServiceNameRegex is the regular expression that outgoing service names must match.
var ServiceNameRegex = regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)

// Get implements [operatorservice.OperatorServiceServer.GetNexusOutgoingService].
func (h *OutgoingServiceRegistry) Get(
	ctx context.Context,
	req *operatorservice.GetNexusOutgoingServiceRequest,
) (*operatorservice.GetNexusOutgoingServiceResponse, error) {
	if err := findIssuesForCommonRequest(req).GetError(); err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}

	var services []*persistencespb.NexusOutgoingService
	if response.Namespace != nil {
		services = response.Namespace.OutgoingServices
	}
	i := h.sortedSetManager.Get(services, req.Name)
	if i < 0 {
		return nil, status.Errorf(codes.NotFound, "outgoing service %q not found", req.Name)
	}
	service := persistenceServiceToAPIService(services[i])
	return &operatorservice.GetNexusOutgoingServiceResponse{
		Service: service,
	}, nil
}

// Create implements [operatorservice.OperatorServiceServer.CreateNexusOutgoingService].
func (h *OutgoingServiceRegistry) Create(
	ctx context.Context,
	req *operatorservice.CreateNexusOutgoingServiceRequest,
) (*operatorservice.CreateNexusOutgoingServiceResponse, error) {
	if err := h.validateUpsertRequest(req); err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.GetNamespace(),
	})
	if err != nil {
		return nil, err
	}
	ns := response.Namespace
	name := req.GetName()
	var created bool
	newService := &persistencespb.NexusOutgoingService{
		Version: 1,
		Name:    name,
		Spec:    req.GetSpec(),
	}
	ns.OutgoingServices, created = h.sortedSetManager.Add(ns.OutgoingServices, newService)
	if !created {
		return nil, status.Errorf(codes.AlreadyExists, "outgoing service %q already exists", name)
	}
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.CreateNexusOutgoingServiceResponse{
		Service: persistenceServiceToAPIService(newService),
	}, nil
}

// Update implements [operatorservice.OperatorServiceServer.UpdateNexusOutgoingService].
func (h *OutgoingServiceRegistry) Update(
	ctx context.Context,
	req *operatorservice.UpdateNexusOutgoingServiceRequest,
) (*operatorservice.UpdateNexusOutgoingServiceResponse, error) {
	if err := h.validateUpsertRequest(req); err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.GetNamespace(),
	})
	if err != nil {
		return nil, err
	}
	ns := response.Namespace
	i := h.sortedSetManager.Get(ns.OutgoingServices, req.Name)
	if i < 0 {
		return nil, status.Errorf(codes.NotFound, "outgoing service %q", req.GetName())
	}
	svc := ns.OutgoingServices[i]
	if svc.Version != req.GetVersion() {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"outgoing service %q version %d does not match expected version %d",
			req.GetName(),
			svc.Version,
			req.GetVersion(),
		)
	}
	svc.Spec = req.GetSpec()
	svc.Version++
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.UpdateNexusOutgoingServiceResponse{
		Service: persistenceServiceToAPIService(svc),
	}, nil
}

// Delete implements [operatorservice.OperatorServiceServer.DeleteNexusOutgoingService].
func (h *OutgoingServiceRegistry) Delete(
	ctx context.Context,
	req *operatorservice.DeleteNexusOutgoingServiceRequest,
) (*operatorservice.DeleteNexusOutgoingServiceResponse, error) {
	if err := findIssuesForCommonRequest(req).GetError(); err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}
	ns := response.Namespace
	var found bool
	ns.OutgoingServices, found = h.sortedSetManager.Remove(ns.OutgoingServices, req.Name)
	if !found {
		return nil, status.Errorf(codes.NotFound, "outgoing service %q", req.Name)
	}
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.DeleteNexusOutgoingServiceResponse{}, nil
}

// List implements [operatorservice.OperatorServiceServer.ListNexusOutgoingServices].
func (h *OutgoingServiceRegistry) List(
	ctx context.Context,
	req *operatorservice.ListNexusOutgoingServicesRequest,
) (*operatorservice.ListNexusOutgoingServicesResponse, error) {
	lastServiceName, pageSize, err := h.parseListRequest(req)
	if err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}
	ns := response.Namespace
	page, lastKey := h.sortedSetManager.Paginate(ns.OutgoingServices, lastServiceName, pageSize)
	services := make([]*nexus.OutgoingService, len(page))
	for i := 0; i < len(page); i++ {
		services[i] = persistenceServiceToAPIService(page[i])
	}
	var nextPageToken []byte
	if lastKey != nil {
		token := outgoingServicesPageToken{LastServiceName: *lastKey}
		nextPageToken = token.Serialize()
	}
	return &operatorservice.ListNexusOutgoingServicesResponse{
		Services:      services,
		NextPageToken: nextPageToken,
	}, nil
}

func (h *OutgoingServiceRegistry) parseListRequest(
	req *operatorservice.ListNexusOutgoingServicesRequest,
) (lastServiceName string, pageSize int, err error) {
	var issues rpc.RequestIssues
	if req.Namespace == "" {
		issues.Append(IssueNamespaceNotSet)
	}
	pageSize = int(req.PageSize)
	if pageSize < 0 {
		issues.Appendf("page_size must be non-negative but is %d", pageSize)
	}
	if pageSize == 0 {
		pageSize = h.config.DefaultPageSize()
	}
	maxPageSize := h.config.MaxPageSize()
	if pageSize > maxPageSize {
		issues.Appendf("page_size cannot exceed %d but is %d", maxPageSize, pageSize)
	}
	var pageToken outgoingServicesPageToken
	if len(req.PageToken) > 0 {
		if err := pageToken.Deserialize(req.PageToken); err != nil {
			issues.Appendf("invalid page_token: %v", err)
		}
	}
	if err := issues.GetError(); err != nil {
		return "", 0, err
	}
	return pageToken.LastServiceName, pageSize, nil
}

func (h *OutgoingServiceRegistry) updateNamespace(
	ctx context.Context,
	ns *persistencespb.NamespaceDetail,
	response *persistence.GetNamespaceResponse,
) error {
	ns.ConfigVersion++
	err := h.namespaceService.UpdateNamespace(ctx, &persistence.UpdateNamespaceRequest{
		Namespace:         ns,
		IsGlobalNamespace: response.IsGlobalNamespace,
		// TODO: This is wrong. It's not how it's done in namespace_handler.
		NotificationVersion: response.NotificationVersion + 1,
	})
	if err != nil {
		return err
	}
	return h.namespaceReplicator.HandleTransmissionTask(
		ctx,
		enums.NAMESPACE_OPERATION_UPDATE,
		ns.GetInfo(),
		ns.GetConfig(),
		ns.GetReplicationConfig(),
		ns.GetOutgoingServices(),
		false,
		ns.ConfigVersion,
		ns.GetFailoverVersion(),
		response.IsGlobalNamespace,
		nil,
	)
}

func persistenceServiceToAPIService(outgoingService *persistencespb.NexusOutgoingService) *nexus.OutgoingService {
	return &nexus.OutgoingService{
		Name:    outgoingService.Name,
		Version: outgoingService.Version,
		Spec:    outgoingService.Spec,
	}
}

type commonRequest interface {
	GetNamespace() string
	GetName() string
}

func findIssuesForCommonRequest(req commonRequest) *rpc.RequestIssues {
	var issues rpc.RequestIssues
	if req.GetNamespace() == "" {
		issues.Append(IssueNamespaceNotSet)
	}
	if req.GetName() == "" {
		issues.Append(IssueNameNotSet)
	}
	return &issues
}

type upsertRequest interface {
	commonRequest
	GetSpec() *nexus.OutgoingServiceSpec
}

func (h *OutgoingServiceRegistry) validateUpsertRequest(req upsertRequest) error {
	issues := findIssuesForCommonRequest(req)
	nameMaxLength := h.config.MaxNameLength()
	if len(req.GetName()) > nameMaxLength {
		issues.Appendf("outgoing service name length exceeds the limit of %d", nameMaxLength)
	}
	if !ServiceNameRegex.MatchString(req.GetName()) {
		issues.Appendf("outgoing service name must match the regex: %q", ServiceNameRegex.String())
	}
	if req.GetSpec() == nil {
		issues.Appendf(IssueSpecNotSet)
	} else {
		if req.GetSpec().GetUrl() == "" {
			issues.Appendf(IssueURLNotSet)
		} else {
			urlMaxLength := h.config.MaxURLLength()
			if len(req.GetSpec().GetUrl()) > urlMaxLength {
				issues.Appendf("outgoing service URL length exceeds the limit of %d", urlMaxLength)
			}
			u, err := url.Parse(req.GetSpec().GetUrl())
			if err != nil {
				issues.Appendf("malformed outgoing service URL: %v", err)
			} else {
				if u.Scheme != "http" && u.Scheme != "https" {
					issues.Appendf("outgoing service URL must have http or https scheme but has %q", u.Scheme)
				}
			}
		}

		if req.GetSpec().GetPublicCallbackUrl() == "" {
			issues.Appendf(IssuePublicCallbackURLNotSet)
		} else {
			urlMaxLength := h.config.MaxURLLength()
			if len(req.GetSpec().GetPublicCallbackUrl()) > urlMaxLength {
				issues.Appendf("outgoing service public callback URL length exceeds the limit of %d", urlMaxLength)
			}
			u, err := url.Parse(req.GetSpec().GetPublicCallbackUrl())
			if err != nil {
				issues.Appendf("malformed outgoing service public callback URL: %v", err)
			} else {
				if u.Scheme != "http" && u.Scheme != "https" {
					issues.Appendf("outgoing service public callback URL must have http or https scheme but has %q", u.Scheme)
				}
			}
		}
	}
	return issues.GetError()
}

type outgoingServicesPageToken struct {
	LastServiceName string `json:"lastServiceName"`
}

func (token *outgoingServicesPageToken) Serialize() []byte {
	b, _ := json.Marshal(token)
	return b
}

func (token *outgoingServicesPageToken) Deserialize(b []byte) error {
	return json.Unmarshal(b, token)
}
