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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	cnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
)

// ServiceNameRegex is the regular expression that incoming service names must match.
var ServiceNameRegex = regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*`)

type (
	// NexusIncomingServiceClient manages frontend CRUD requests for Nexus incoming services.
	// Create, Update, and Delete requests are forwarded to matching service which owns the incoming services table.
	// Read (Get and List) requests are sent directly to persistence. This is to ensure read-after-write consistency.
	NexusIncomingServiceClient struct {
		config *nexusIncomingServiceClientConfig

		namespaceRegistry namespace.Registry // used to validate referenced namespaces exist
		matchingClient    matchingservice.MatchingServiceClient
		persistence       p.NexusIncomingServiceManager

		logger log.Logger
	}

	nexusIncomingServiceClientConfig struct {
		maxNameLength       dynamicconfig.IntPropertyFn
		maxTaskQueueLength  dynamicconfig.IntPropertyFn
		maxSize             dynamicconfig.IntPropertyFn
		listDefaultPageSize dynamicconfig.IntPropertyFn
		listMaxPageSize     dynamicconfig.IntPropertyFn
	}
)

func newNexusIncomingServiceClientConfig(dc *dynamicconfig.Collection) *nexusIncomingServiceClientConfig {
	return &nexusIncomingServiceClientConfig{
		maxNameLength:       dynamicconfig.NexusIncomingServiceNameMaxLength.Get(dc),
		maxTaskQueueLength:  dynamicconfig.MaxIDLengthLimit.Get(dc),
		maxSize:             dynamicconfig.NexusIncomingServiceMaxSize.Get(dc),
		listDefaultPageSize: dynamicconfig.NexusIncomingServiceListDefaultPageSize.Get(dc),
		listMaxPageSize:     dynamicconfig.NexusIncomingServiceListMaxPageSize.Get(dc),
	}
}

func newNexusIncomingServiceClient(
	config *nexusIncomingServiceClientConfig,
	namespaceRegistry namespace.Registry,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusIncomingServiceManager,
	logger log.Logger,
) *NexusIncomingServiceClient {
	return &NexusIncomingServiceClient{
		config:            config,
		namespaceRegistry: namespaceRegistry,
		matchingClient:    matchingClient,
		persistence:       persistence,
		logger:            logger,
	}
}

func (c *NexusIncomingServiceClient) Create(
	ctx context.Context,
	request *operatorservice.CreateNexusIncomingServiceRequest,
) (*operatorservice.CreateNexusIncomingServiceResponse, error) {
	if err := c.validateUpsertSpec(request.GetSpec()); err != nil {
		return nil, err
	}

	resp, err := c.matchingClient.CreateNexusIncomingService(ctx, &matchingservice.CreateNexusIncomingServiceRequest{
		Spec: request.Spec,
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.CreateNexusIncomingServiceResponse{
		Service: resp.GetService(),
	}, nil
}

func (c *NexusIncomingServiceClient) Update(
	ctx context.Context,
	request *operatorservice.UpdateNexusIncomingServiceRequest,
) (*operatorservice.UpdateNexusIncomingServiceResponse, error) {
	if err := c.validateUpsertSpec(request.GetSpec()); err != nil {
		return nil, err
	}

	resp, err := c.matchingClient.UpdateNexusIncomingService(ctx, &matchingservice.UpdateNexusIncomingServiceRequest{
		Id:      request.Id,
		Version: request.Version,
		Spec:    request.Spec,
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.UpdateNexusIncomingServiceResponse{
		Service: resp.Service,
	}, nil
}

func (c *NexusIncomingServiceClient) Delete(
	ctx context.Context,
	request *operatorservice.DeleteNexusIncomingServiceRequest,
) (*operatorservice.DeleteNexusIncomingServiceResponse, error) {
	if err := validateDeleteRequest(request); err != nil {
		return nil, err
	}

	_, err := c.matchingClient.DeleteNexusIncomingService(ctx, &matchingservice.DeleteNexusIncomingServiceRequest{
		Id: request.Id,
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.DeleteNexusIncomingServiceResponse{}, nil
}

func (c *NexusIncomingServiceClient) Get(
	ctx context.Context,
	request *operatorservice.GetNexusIncomingServiceRequest,
) (*operatorservice.GetNexusIncomingServiceResponse, error) {
	if err := validateGetRequest(request); err != nil {
		return nil, err
	}

	entry, err := c.persistence.GetNexusIncomingService(ctx, &p.GetNexusIncomingServiceRequest{
		ServiceID: request.Id,
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, err
		}
		c.logger.Error(fmt.Sprintf("error looking up Nexus incoming service with ID `%v` from persistence", request.Id), tag.Error(err))
		return nil, serviceerror.NewInternal(fmt.Sprintf("error looking up Nexus incoming service with ID `%v`", request.Id))
	}

	return &operatorservice.GetNexusIncomingServiceResponse{
		Service: cnexus.IncomingServicePersistedEntryToExternalAPI(entry),
	}, nil
}

func (c *NexusIncomingServiceClient) List(
	ctx context.Context,
	request *operatorservice.ListNexusIncomingServicesRequest,
) (*operatorservice.ListNexusIncomingServicesResponse, error) {
	if request.GetName() != "" {
		return c.listAndFilterByName(ctx, request)
	}

	pageSize := request.GetPageSize()
	if pageSize == 0 {
		pageSize = int32(c.config.listDefaultPageSize())
	} else if err := c.validatePageSize(pageSize); err != nil {
		return nil, err
	}

	resp, err := c.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: 0,
		NextPageToken:         request.NextPageToken,
		PageSize:              int(pageSize),
	})
	if err != nil {
		c.logger.Error(fmt.Sprintf("error listing Nexus incoming services from persistence. NextPageToken: %v PageSize: %d", request.NextPageToken, pageSize), tag.Error(err))
		return nil, serviceerror.NewInternal("error listing Nexus incoming services")
	}

	services := make([]*nexus.IncomingService, len(resp.Entries))
	for i, entry := range resp.Entries {
		services[i] = cnexus.IncomingServicePersistedEntryToExternalAPI(entry)
	}

	return &operatorservice.ListNexusIncomingServicesResponse{
		NextPageToken: resp.NextPageToken,
		Services:      services,
	}, nil
}

// listAndFilterByName paginates over all services returned by persistence layer to find the service name
// indicated in the request. Returns that service if found or an empty response if not.
// PageSize and NextPageToken fields on the request are ignored.
func (c *NexusIncomingServiceClient) listAndFilterByName(
	ctx context.Context,
	request *operatorservice.ListNexusIncomingServicesRequest,
) (*operatorservice.ListNexusIncomingServicesResponse, error) {
	result := &operatorservice.ListNexusIncomingServicesResponse{Services: []*nexus.IncomingService{}}
	pageSize := c.config.listDefaultPageSize()
	var currentPageToken []byte

	for ctx.Err() == nil {
		resp, err := c.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: 0,
			NextPageToken:         currentPageToken,
			PageSize:              pageSize,
		})
		if err != nil {
			c.logger.Error(fmt.Sprintf("error listing Nexus incoming services from persistence with Name filter. CurrentPageToken: %v PageSize: %d ServiceName: %v", currentPageToken, pageSize, request.Name), tag.Error(err))
			return nil, serviceerror.NewInternal("error listing Nexus incoming services")
		}

		for _, entry := range resp.Entries {
			if request.Name == entry.Service.Spec.Name {
				result.Services = []*nexus.IncomingService{cnexus.IncomingServicePersistedEntryToExternalAPI(entry)}
				return result, nil
			}
		}

		if resp.NextPageToken == nil {
			return result, nil
		}

		currentPageToken = resp.NextPageToken
	}

	return nil, ctx.Err()
}

func (c *NexusIncomingServiceClient) getServiceNameIssues(name string) rpc.RequestIssues {
	var issues rpc.RequestIssues

	if name == "" {
		issues.Append("incoming service name not set")
		return issues
	}

	maxNameLength := c.config.maxNameLength()
	if len(name) > maxNameLength {
		issues.Appendf("incoming service name exceeds length limit of %d", maxNameLength)
	}

	if !ServiceNameRegex.MatchString(name) {
		issues.Appendf("incoming service name must match the regex: %q", ServiceNameRegex.String())
	}

	return issues
}

func (c *NexusIncomingServiceClient) validateUpsertSpec(spec *nexus.IncomingServiceSpec) error {
	issues := c.getServiceNameIssues(spec.GetName())

	if spec.Namespace == "" {
		issues.Append("incoming service namespace not set")
	} else if _, nsErr := c.namespaceRegistry.GetNamespace(namespace.Name(spec.Namespace)); nsErr != nil {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("could not verify namespace referenced by incoming service exists: %v", nsErr.Error()))
	}

	if err := validateTaskQueueName(spec.GetTaskQueue(), c.config.maxTaskQueueLength()); err != nil {
		issues.Appendf("invalid incoming service task queue: %q", err.Error())
	}

	maxSize := c.config.maxSize()
	if spec.Size() > maxSize {
		issues.Appendf("incoming service size exceeds limit of %d", maxSize)
	}

	return issues.GetError()
}

func getServiceIDIssues(ID string) rpc.RequestIssues {
	var issues rpc.RequestIssues
	if ID == "" {
		issues.Append("incoming service ID not set")
	} else if _, err := uuid.Parse(ID); err != nil {
		issues.Appendf("malformed incoming service ID: %q", err.Error())
	}
	return issues
}

func validateDeleteRequest(request *operatorservice.DeleteNexusIncomingServiceRequest) error {
	issues := getServiceIDIssues(request.GetId())

	if request.GetVersion() <= 0 {
		issues.Append("incoming service version is non-positive")
	}

	return issues.GetError()
}

func validateGetRequest(request *operatorservice.GetNexusIncomingServiceRequest) error {
	issues := getServiceIDIssues(request.GetId())
	return issues.GetError()
}

func (c *NexusIncomingServiceClient) validatePageSize(pageSize int32) error {
	// pageSize == 0 is treated as unset and will be changed to the default and does not go through this validation
	if pageSize < 0 {
		return serviceerror.NewInvalidArgument("page_size is negative")
	}

	maxPageSize := c.config.listMaxPageSize()
	if pageSize > int32(maxPageSize) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("page_size exceeds limit of %d", maxPageSize))
	}

	return nil
}
