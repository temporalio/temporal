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
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/slicex"
	"go.uber.org/multierr"
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
		MaxURLLength:    dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceURLMaxLength, 1000),
		NameMaxLength:   dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceNameMaxLength, 200),
		DefaultPageSize: dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceListDefaultPageSize, 100),
		MaxPageSize:     dc.GetIntProperty(dynamicconfig.NexusOutgoingServiceListMaxPageSize, 1000),
	}
}

var (
	ErrNamespaceNotSet = status.Errorf(codes.InvalidArgument, "Namespace is not set on request")
	ErrNameNotSet      = status.Errorf(codes.InvalidArgument, "Name is not set on request")
	ErrURLNotSet       = status.Errorf(codes.InvalidArgument, "URL is not set on request")
)

var ServiceNameRegex = regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)

func (h *OutgoingServiceRegistry) Get(
	ctx context.Context,
	req *operatorservice.GetNexusOutgoingServiceRequest,
) (*operatorservice.GetNexusOutgoingServiceResponse, error) {
	if err := validateCommonRequest(req); err != nil {
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
	i := slices.IndexFunc(ns.OutgoingServices, func(svc *persistencespb.NexusOutgoingService) bool {
		return svc.Name == req.GetName()
	})
	if i >= 0 {
		return nil, status.Errorf(
			codes.AlreadyExists,
			"Outgoing service %q already exists with version %d",
			req.GetName(),
			ns.OutgoingServices[i].Version,
		)
	}
	ns.OutgoingServices = append(ns.OutgoingServices, &persistencespb.NexusOutgoingService{
		Version: 1,
		Name:    req.GetName(),
		Spec:    common.CloneProto(req.GetSpec()),
	})
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.CreateNexusOutgoingServiceResponse{
		Service: &nexus.OutgoingService{
			Name:    req.GetName(),
			Version: 1,
			Spec:    req.GetSpec(),
		},
	}, nil
}

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
	i := slices.IndexFunc(ns.OutgoingServices, func(svc *persistencespb.NexusOutgoingService) bool {
		return svc.Name == req.GetName()
	})
	if i < 0 {
		return nil, status.Errorf(codes.NotFound, "Outgoing service %q not found", req.GetName())
	}
	service := ns.OutgoingServices[i]
	if service.Version != req.GetVersion() {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"Outgoing service %q version %d does not match expected version %d",
			req.GetName(),
			service.Version,
			req.GetVersion(),
		)
	}
	service.Version++
	service.Spec = common.CloneProto(req.GetSpec())
	if err := h.updateNamespace(ctx, ns, response); err != nil {
		return nil, err
	}
	return &operatorservice.UpdateNexusOutgoingServiceResponse{
		Service: &nexus.OutgoingService{
			Name:    req.GetName(),
			Version: service.Version,
			Spec:    req.GetSpec(),
		},
	}, nil
}

func (h *OutgoingServiceRegistry) Delete(
	ctx context.Context,
	req *operatorservice.DeleteNexusOutgoingServiceRequest,
) (*operatorservice.DeleteNexusOutgoingServiceResponse, error) {
	if err := validateCommonRequest(req); err != nil {
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
	ns.OutgoingServices = slices.DeleteFunc(services, func(svc *persistencespb.NexusOutgoingService) bool {
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

func findIndexOfNextService(services []*persistencespb.NexusOutgoingService, lastServiceName string) int {
	i := slicex.LowerBoundFunc(services, lastServiceName, func(svc *persistencespb.NexusOutgoingService, name string) int {
		return strings.Compare(svc.Name, name)
	})
	if i < len(services) && services[i].Name == lastServiceName {
		i++
	}
	return i
}

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
	startIndex := findIndexOfNextService(response.Namespace.OutgoingServices, lastServiceName)
	size := min(pageSize, len(response.Namespace.OutgoingServices)-startIndex)
	if size <= 0 {
		return &operatorservice.ListNexusOutgoingServicesResponse{}, nil
	}
	services := make([]*nexus.OutgoingService, size)
	for i := 0; i < size; i++ {
		service := response.Namespace.OutgoingServices[startIndex+i]
		services[i] = &nexus.OutgoingService{
			Name:    service.Name,
			Version: service.Version,
			Spec:    common.CloneProto(service.Spec),
		}
	}
	var nextPageToken []byte
	if startIndex+pageSize < len(response.Namespace.OutgoingServices) {
		token := outgoingServicesPageToken{LastServiceName: services[len(services)-1].Name}
		nextPageToken = token.Serialize()
	}
	return &operatorservice.ListNexusOutgoingServicesResponse{
		Services:      services,
		NextPageToken: nextPageToken,
	}, nil
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

func validateCommonRequest(req commonRequest) error {
	var errs error
	if req, ok := req.(interface{ GetNamespace() string }); ok && req.GetNamespace() == "" {
		errs = multierr.Append(errs, ErrNamespaceNotSet)
	}
	if req, ok := req.(interface{ GetName() string }); ok && req.GetName() == "" {
		errs = multierr.Append(errs, ErrNameNotSet)
	}
	return errs
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

func (h *OutgoingServiceRegistry) addService(req upsertRequest, version int64, ns *persistencespb.NamespaceDetail) error {
	if version != 0 {
		return status.Errorf(
			codes.NotFound,
			"Outgoing service %q not found. Set Version to 0 (not %d) if you want to register a new service.",
			req.GetName(),
			version,
		)
	}
	ns.OutgoingServices = append(ns.OutgoingServices, &persistencespb.NexusOutgoingService{
		Version: 1,
		Name:    req.GetName(),
		Spec:    common.CloneProto(req.GetSpec()),
	})
	return nil
}

func (h *OutgoingServiceRegistry) parseListRequest(
	req *operatorservice.ListNexusOutgoingServicesRequest,
) (lastServiceName string, pageSize int, err error) {
	var errs error
	if req.Namespace == "" {
		errs = multierr.Append(errs, ErrNamespaceNotSet)
	}
	pageSize = int(req.PageSize)
	if pageSize < 0 {
		errs = multierr.Append(errs, status.Errorf(codes.InvalidArgument, "PageSize must be non-negative but is %d", pageSize))
	}
	if pageSize == 0 {
		pageSize = h.config.DefaultPageSize()
	}
	maxPageSize := h.config.MaxPageSize()
	if pageSize > maxPageSize {
		errs = multierr.Append(errs, status.Errorf(
			codes.InvalidArgument,
			"PageSize cannot exceed %d but is %d",
			maxPageSize,
			pageSize,
		))
	}
	var pageToken outgoingServicesPageToken
	if len(req.PageToken) > 0 {
		if err := pageToken.Deserialize(req.PageToken); err != nil {
			errs = multierr.Append(errs, status.Errorf(
				codes.InvalidArgument,
				"Invalid NextPageToken: %v",
				err,
			))
		}
	}
	if errs != nil {
		return "", 0, errs
	}
	return pageToken.LastServiceName, pageSize, nil
}

func (h *OutgoingServiceRegistry) findService(
	response *persistence.GetNamespaceResponse,
	serviceName string,
) *nexus.OutgoingService {
	var services []*persistencespb.NexusOutgoingService
	if detail := response.Namespace; detail != nil {
		services = detail.OutgoingServices
	}
	for _, service := range services {
		if service.Name == serviceName {
			return &nexus.OutgoingService{
				Name:    service.Name,
				Version: service.Version,
				Spec:    common.CloneProto(service.Spec),
			}
		}
	}
	return nil
}

type commonRequest interface {
	GetNamespace() string
	GetName() string
}

type upsertRequest interface {
	commonRequest
	GetSpec() *nexus.OutgoingServiceSpec
}

func (h *OutgoingServiceRegistry) validateUpsertRequest(req upsertRequest) error {
	var errs error
	if err := validateCommonRequest(req); err != nil {
		errs = multierr.Append(errs, err)
	}
	nameMaxLength := h.config.NameMaxLength()
	if len(req.GetName()) > nameMaxLength {
		errs = multierr.Append(errs, status.Errorf(
			codes.InvalidArgument,
			"Outgoing service name length exceeds the limit of %d",
			nameMaxLength,
		))
	}
	if !ServiceNameRegex.MatchString(req.GetName()) {
		errs = multierr.Append(errs, status.Errorf(
			codes.InvalidArgument,
			"Outgoing service name must match the regex: %q",
			ServiceNameRegex.String(),
		))
	}
	if req.GetSpec() == nil || req.GetSpec().GetUrl() == "" {
		errs = multierr.Append(errs, ErrURLNotSet)
	} else {
		urlMaxLength := h.config.MaxURLLength()
		if len(req.GetSpec().GetUrl()) > urlMaxLength {
			errs = multierr.Append(errs, status.Errorf(
				codes.InvalidArgument,
				"Outgoing service URL length exceeds the limit of %d",
				urlMaxLength,
			))
		}
		u, err := url.Parse(req.GetSpec().GetUrl())
		if err != nil {
			errs = multierr.Append(errs, status.Errorf(
				codes.InvalidArgument,
				"Malformed outgoing service URL: %v",
				err,
			))
		} else {
			if u.Scheme != "http" && u.Scheme != "https" {
				errs = multierr.Append(errs, status.Errorf(
					codes.InvalidArgument,
					"Outgoing service URL must have http or https scheme but has %q",
					u.Scheme,
				))
			}
		}
	}
	return errs
}
