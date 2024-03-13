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

package nexus

import (
	"context"
	"fmt"
	"net/url"
	"regexp"

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OutgoingServiceRegistry manages the registration and retrieval of "outgoing" services from the namespace metadata in
// the persistence layer. An outgoing service is a Nexus service to which we send traffic. For example, we might call
// one of these services to start an operation using this API: https://github.com/nexus-rpc/api/blob/main/SPEC.md#start-operation.
// We need a registry for these services, so that we can look up the URL for a service by name. Later, we may add more
// service-specific information to the registry.
type OutgoingServiceRegistry struct {
	namespaceService NamespaceService
	config           *OutgoingServiceRegistryConfig
}

// NamespaceService is an interface which contains only the methods we need from
// [go.temporal.io/server/common/persistence.MetadataManager].
type NamespaceService interface {
	CreateNamespace(ctx context.Context, request *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error)
	GetNamespace(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error)
	UpdateNamespace(ctx context.Context, request *persistence.UpdateNamespaceRequest) error
}

// NewOutgoingServiceRegistry creates a new OutgoingServiceRegistry with the given namespace service and configuration.
func NewOutgoingServiceRegistry(
	namespaceService NamespaceService,
	config *OutgoingServiceRegistryConfig,
) *OutgoingServiceRegistry {
	return &OutgoingServiceRegistry{
		namespaceService: namespaceService,
		config:           config,
	}
}

type OutgoingServiceRegistryConfig struct {
	MaxURLLength    dynamicconfig.IntPropertyFn
	NameMaxLength   dynamicconfig.IntPropertyFn
	DefaultPageSize dynamicconfig.IntPropertyFn
	MaxPageSize     dynamicconfig.IntPropertyFn
}

func NewOutgoingServiceRegistryConfig(dc *dynamicconfig.Collection) *OutgoingServiceRegistryConfig {
	return &OutgoingServiceRegistryConfig{
		MaxURLLength:    dc.GetIntProperty(dynamicconfig.OutgoingServiceURLMaxLength, 1000),
		NameMaxLength:   dc.GetIntProperty(dynamicconfig.OutgoingServiceNameMaxLength, 200),
		DefaultPageSize: dc.GetIntProperty(dynamicconfig.OutgoingServiceListDefaultPageSize, 100),
		MaxPageSize:     dc.GetIntProperty(dynamicconfig.OutgoingServiceListMaxPageSize, 1000),
	}
}

var (
	ErrNamespaceNotSet = status.Errorf(codes.InvalidArgument, "Namespace is not set on request")
	ErrNameNotSet      = status.Errorf(codes.InvalidArgument, "Name is not set on request")
	ErrURLNotSet       = status.Errorf(codes.InvalidArgument, "URL is not set on request")
)

var serviceNameRegex = regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)

func (h *OutgoingServiceRegistry) Get(
	ctx context.Context,
	req *operatorservice.GetNexusOutgoingServiceRequest,
) (*operatorservice.GetNexusOutgoingServiceResponse, error) {
	if err := validateCommonRequestParams(req); err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}

	service := h.findService(response, req.Name)
	if service == nil {
		return nil, status.Errorf(codes.NotFound, "outgoing service %q not found", req.Name)
	}
	return &operatorservice.GetNexusOutgoingServiceResponse{
		Service: service,
	}, nil
}

func (h *OutgoingServiceRegistry) Upsert(
	ctx context.Context,
	req *operatorservice.CreateOrUpdateNexusOutgoingServiceRequest,
) (*operatorservice.CreateOrUpdateNexusOutgoingServiceResponse, error) {
	if err := h.validateUpsertRequest(req); err != nil {
		return nil, err
	}
	return h.upsertService(ctx, req)
}

func (h *OutgoingServiceRegistry) Delete(
	ctx context.Context,
	req *operatorservice.DeleteNexusOutgoingServiceRequest,
) (*operatorservice.DeleteNexusOutgoingServiceResponse, error) {
	if err := validateCommonRequestParams(req); err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}
	ns := response.Namespace
	services := ns.OutgoingServices
	ns.OutgoingServices = slices.DeleteFunc(services, func(svc *persistencespb.OutgoingService) bool {
		return svc.Name == req.Name
	})
	if len(services) == len(ns.OutgoingServices) {
		return nil, status.Errorf(codes.NotFound, "Outgoing service %q not found", req.Name)
	}
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.DeleteNexusOutgoingServiceResponse{}, nil
}

func (h *OutgoingServiceRegistry) List(
	ctx context.Context,
	req *operatorservice.ListNexusOutgoingServicesRequest,
) (*operatorservice.ListNexusOutgoingServicesResponse, error) {
	startIndex, pageSize, err := h.parseListRequest(req)
	if err != nil {
		return nil, err
	}
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}
	numReturnedServices := min(pageSize, len(response.Namespace.OutgoingServices)-startIndex)
	if numReturnedServices <= 0 {
		return &operatorservice.ListNexusOutgoingServicesResponse{}, nil
	}
	services := make([]*nexus.OutgoingService, 0, numReturnedServices)
	for i := startIndex; i < startIndex+numReturnedServices; i++ {
		index := startIndex + i
		service := response.Namespace.OutgoingServices[index]
		services = append(services, &nexus.OutgoingService{
			Name:    service.Name,
			Version: service.Version,
			Url:     service.Url,
		})
	}
	var nextPageToken []byte
	if startIndex+pageSize < len(response.Namespace.OutgoingServices) {
		token := outgoingServicesPageToken{Index: startIndex + pageSize}
		nextPageToken = token.Serialize()
	}
	return &operatorservice.ListNexusOutgoingServicesResponse{
		Services:      services,
		NextPageToken: nextPageToken,
	}, nil
}

type outgoingServicesPageToken struct {
	Index int
}

func (token *outgoingServicesPageToken) Serialize() []byte {
	return []byte(fmt.Sprintf("v1/%d", token.Index))
}

func (token *outgoingServicesPageToken) Deserialize(b []byte) error {
	_, err := fmt.Sscanf(string(b), "v1/%d", &token.Index)
	return err
}

func validateCommonRequestParams(req any) error {
	if req, ok := req.(interface{ GetNamespace() string }); ok && req.GetNamespace() == "" {
		return ErrNamespaceNotSet
	}
	if req, ok := req.(interface{ GetName() string }); ok && req.GetName() == "" {
		return ErrNameNotSet
	}
	return nil
}

func (h *OutgoingServiceRegistry) upsertService(
	ctx context.Context,
	req *operatorservice.CreateOrUpdateNexusOutgoingServiceRequest,
) (*operatorservice.CreateOrUpdateNexusOutgoingServiceResponse, error) {
	response, err := h.namespaceService.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: req.Namespace,
	})
	if err != nil {
		return nil, err
	}
	ns := response.Namespace
	i := slices.IndexFunc(ns.OutgoingServices, func(svc *persistencespb.OutgoingService) bool {
		return svc.Name == req.Name
	})
	if i < 0 {
		if err := h.addService(req, ns); err != nil {
			return nil, err
		}
	} else {
		if err := h.updateService(req, ns.OutgoingServices[i]); err != nil {
			return nil, err
		}
	}
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.CreateOrUpdateNexusOutgoingServiceResponse{
		Service: &nexus.OutgoingService{
			Name:    req.Name,
			Version: req.Version + 1, // This is the version now in the DB.
			Url:     req.Url,
		},
	}, nil
}

func (h *OutgoingServiceRegistry) updateNamespace(
	ctx context.Context,
	ns *persistencespb.NamespaceDetail,
	response *persistence.GetNamespaceResponse,
) error {
	return h.namespaceService.UpdateNamespace(ctx, &persistence.UpdateNamespaceRequest{
		Namespace:           ns,
		IsGlobalNamespace:   response.IsGlobalNamespace,
		NotificationVersion: response.NotificationVersion + 1,
	})
}

func (h *OutgoingServiceRegistry) addService(req *operatorservice.CreateOrUpdateNexusOutgoingServiceRequest, ns *persistencespb.NamespaceDetail) error {
	if req.Version != 0 {
		return status.Errorf(
			codes.NotFound,
			"Outgoing service %q not found. Set Version to 0 (not %d) if you want to register a new service.",
			req.Name,
			req.Version,
		)
	}
	ns.OutgoingServices = append(ns.OutgoingServices, &persistencespb.OutgoingService{
		Version: 1,
		Name:    req.Name,
		Url:     req.Url,
	})
	return nil
}

func (h *OutgoingServiceRegistry) updateService(req *operatorservice.CreateOrUpdateNexusOutgoingServiceRequest, service *persistencespb.OutgoingService) error {
	if req.Version == 0 {
		return status.Errorf(
			codes.AlreadyExists,
			"Outgoing service %q already exists. Set Version to a non-zero value if you want to update it.",
			req.Name,
		)
	}
	if service.Version != req.Version {
		return status.Errorf(
			codes.FailedPrecondition,
			"Outgoing service %q update request version %d does not match the current version %d",
			req.Name,
			req.Version,
			service.Version,
		)
	}
	// The service variable is a pointer, so we can modify it directly
	service.Version++
	service.Url = req.Url
	return nil
}

func (h *OutgoingServiceRegistry) parseListRequest(
	req *operatorservice.ListNexusOutgoingServicesRequest,
) (startIndex int, pageSize int, err error) {
	if err := validateCommonRequestParams(req); err != nil {
		return 0, 0, err
	}
	pageSize = int(req.PageSize)
	if pageSize < 0 {
		return 0, 0, status.Errorf(codes.InvalidArgument, "PageSize must be non-negative but is %d", pageSize)
	}
	if pageSize == 0 {
		pageSize = h.config.DefaultPageSize()
	}
	maxPageSize := h.config.MaxPageSize()
	if pageSize > maxPageSize {
		return 0, 0, status.Errorf(
			codes.InvalidArgument,
			"PageSize cannot exceed %d but is %d",
			maxPageSize,
			pageSize,
		)
	}
	var pageToken outgoingServicesPageToken
	if len(req.NextPageToken) > 0 {
		if err := pageToken.Deserialize(req.NextPageToken); err != nil {
			return 0, 0, status.Errorf(
				codes.InvalidArgument,
				"Invalid NextPageToken: %v",
				err,
			)
		}
	}
	startIndex = pageToken.Index
	if startIndex < 0 {
		return 0, 0, status.Errorf(
			codes.InvalidArgument,
			"Invalid NextPageToken: Index must be non-negative but is %d",
			startIndex,
		)
	}
	return startIndex, pageSize, nil
}

func (h *OutgoingServiceRegistry) findService(
	response *persistence.GetNamespaceResponse,
	serviceName string,
) *nexus.OutgoingService {
	var services []*persistencespb.OutgoingService
	if detail := response.Namespace; detail != nil {
		services = detail.OutgoingServices
	}
	for _, service := range services {
		if service.Name == serviceName {
			return &nexus.OutgoingService{
				Name:    service.Name,
				Version: service.Version,
				Url:     service.Url,
			}
		}
	}
	return nil
}

func (h *OutgoingServiceRegistry) validateUpsertRequest(req *operatorservice.CreateOrUpdateNexusOutgoingServiceRequest) error {
	if err := validateCommonRequestParams(req); err != nil {
		return err
	}
	nameMaxLength := h.config.NameMaxLength()
	if len(req.Name) > nameMaxLength {
		return status.Errorf(codes.InvalidArgument, "Outgoing service name length exceeds the limit of %d", nameMaxLength)
	}
	if !serviceNameRegex.MatchString(req.Name) {
		return status.Errorf(codes.InvalidArgument, "Outgoing service name must match the regex: %q", serviceNameRegex.String())
	}
	if req.Url == "" {
		return ErrURLNotSet
	}
	urlMaxLength := h.config.MaxURLLength()
	if len(req.Url) > urlMaxLength {
		return status.Errorf(codes.InvalidArgument, "Outgoing service URL length exceeds the limit of %d", urlMaxLength)
	}
	u, err := url.Parse(req.Url)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Malformed outgoing service URL: %v", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return status.Errorf(codes.InvalidArgument, "Outgoing service URL must have http or https scheme but has %q", u.Scheme)
	}
	return nil
}
