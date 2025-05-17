package frontend

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/google/uuid"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	cnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EndpointNameRegex is the regular expression that endpoint names must match.
var EndpointNameRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9]$`)

type (
	// NexusEndpointClient manages frontend CRUD requests for Nexus endpoints.
	// Create, Update, and Delete requests are forwarded to matching service which owns the endpoints table.
	// Read (Get and List) requests are sent directly to persistence. This is to ensure read-after-write consistency.
	NexusEndpointClient struct {
		config *nexusEndpointClientConfig

		namespaceRegistry namespace.Registry // used to validate referenced namespaces exist
		matchingClient    matchingservice.MatchingServiceClient
		persistence       p.NexusEndpointManager

		logger log.Logger
	}

	nexusEndpointClientConfig struct {
		maxNameLength                dynamicconfig.IntPropertyFn
		maxTaskQueueLength           dynamicconfig.IntPropertyFn
		maxDescriptionSize           dynamicconfig.IntPropertyFn
		maxExternalEndpointURLLength dynamicconfig.IntPropertyFn
		listDefaultPageSize          dynamicconfig.IntPropertyFn
		listMaxPageSize              dynamicconfig.IntPropertyFn
	}
)

func newNexusEndpointClientConfig(dc *dynamicconfig.Collection) *nexusEndpointClientConfig {
	maxDescriptionSizeFn := dynamicconfig.NexusEndpointDescriptionMaxSize.Get(dc)

	return &nexusEndpointClientConfig{
		maxNameLength:      dynamicconfig.NexusEndpointNameMaxLength.Get(dc),
		maxTaskQueueLength: dynamicconfig.MaxIDLengthLimit.Get(dc),
		maxDescriptionSize: func() int {
			return maxDescriptionSizeFn("") // Ignore namespace for endpoints since they are global resources.
		},
		maxExternalEndpointURLLength: dynamicconfig.NexusEndpointExternalURLMaxLength.Get(dc),
		listDefaultPageSize:          dynamicconfig.NexusEndpointListDefaultPageSize.Get(dc),
		listMaxPageSize:              dynamicconfig.NexusEndpointListMaxPageSize.Get(dc),
	}
}

func newNexusEndpointClient(
	config *nexusEndpointClientConfig,
	namespaceRegistry namespace.Registry,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusEndpointManager,
	logger log.Logger,
) *NexusEndpointClient {
	return &NexusEndpointClient{
		config:            config,
		namespaceRegistry: namespaceRegistry,
		matchingClient:    matchingClient,
		persistence:       persistence,
		logger:            logger,
	}
}

func (c *NexusEndpointClient) Create(
	ctx context.Context,
	request *operatorservice.CreateNexusEndpointRequest,
) (*operatorservice.CreateNexusEndpointResponse, error) {
	if err := c.validateUpsertSpec(request.GetSpec()); err != nil {
		return nil, err
	}

	spec, err := c.apiSpecToPersistenceSpec(request.GetSpec())
	if err != nil {
		return nil, err
	}
	resp, err := c.matchingClient.CreateNexusEndpoint(ctx, &matchingservice.CreateNexusEndpointRequest{
		Spec: spec,
	})
	if err != nil {
		return nil, err
	}

	endpoint, err := c.endpointPersistedEntryToExternalAPI(resp.Entry)
	if err != nil {
		return nil, err
	}

	return &operatorservice.CreateNexusEndpointResponse{
		Endpoint: endpoint,
	}, nil
}

func (c *NexusEndpointClient) Update(
	ctx context.Context,
	request *operatorservice.UpdateNexusEndpointRequest,
) (*operatorservice.UpdateNexusEndpointResponse, error) {
	if err := c.validateUpsertSpec(request.GetSpec()); err != nil {
		return nil, err
	}

	spec, err := c.apiSpecToPersistenceSpec(request.GetSpec())
	if err != nil {
		return nil, err
	}

	resp, err := c.matchingClient.UpdateNexusEndpoint(ctx, &matchingservice.UpdateNexusEndpointRequest{
		Id:      request.Id,
		Version: request.Version,
		Spec:    spec,
	})
	if err != nil {
		return nil, err
	}

	endpoint, err := c.endpointPersistedEntryToExternalAPI(resp.Entry)
	if err != nil {
		return nil, c.transformServiceError(err, "internal error")
	}

	return &operatorservice.UpdateNexusEndpointResponse{
		Endpoint: endpoint,
	}, nil
}

func (c *NexusEndpointClient) Delete(
	ctx context.Context,
	request *operatorservice.DeleteNexusEndpointRequest,
) (*operatorservice.DeleteNexusEndpointResponse, error) {
	if err := validateDeleteRequest(request); err != nil {
		return nil, err
	}

	_, err := c.matchingClient.DeleteNexusEndpoint(ctx, &matchingservice.DeleteNexusEndpointRequest{
		Id: request.Id,
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.DeleteNexusEndpointResponse{}, nil
}

func (c *NexusEndpointClient) Get(
	ctx context.Context,
	request *operatorservice.GetNexusEndpointRequest,
) (*operatorservice.GetNexusEndpointResponse, error) {
	if err := validateGetRequest(request); err != nil {
		return nil, err
	}

	entry, err := c.persistence.GetNexusEndpoint(ctx, &p.GetNexusEndpointRequest{
		ID: request.Id,
	})
	if err != nil {
		return nil, c.transformServiceError(err, fmt.Sprintf("error looking up Nexus endpoint with ID `%v`", request.Id))
	}

	endpoint, err := c.endpointPersistedEntryToExternalAPI(entry)
	if err != nil {
		return nil, c.transformServiceError(err, fmt.Sprintf("error looking up Nexus endpoint with ID `%v`", request.Id))
	}

	return &operatorservice.GetNexusEndpointResponse{
		Endpoint: endpoint,
	}, nil
}

func (c *NexusEndpointClient) List(
	ctx context.Context,
	request *operatorservice.ListNexusEndpointsRequest,
) (*operatorservice.ListNexusEndpointsResponse, error) {
	if request.GetName() != "" {
		return c.listAndFilterByName(ctx, request)
	}

	pageSize := request.GetPageSize()
	if pageSize == 0 {
		pageSize = int32(c.config.listDefaultPageSize())
	} else if err := c.validatePageSize(pageSize); err != nil {
		return nil, err
	}

	resp, err := c.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		NextPageToken:         request.NextPageToken,
		PageSize:              int(pageSize),
	})
	if err != nil {
		c.logger.Error(fmt.Sprintf("error listing Nexus endpoints from persistence. NextPageToken: %v PageSize: %d", request.NextPageToken, pageSize), tag.Error(err))
		return nil, serviceerror.NewInternal("error listing Nexus endpoints")
	}

	endpoints := make([]*nexuspb.Endpoint, len(resp.Entries))
	for i, entry := range resp.Entries {
		endpoint, err := c.endpointPersistedEntryToExternalAPI(entry)
		if err != nil {
			return nil, c.transformServiceError(err, "error listing Nexus endpoints")
		}
		endpoints[i] = endpoint
	}

	return &operatorservice.ListNexusEndpointsResponse{
		NextPageToken: resp.NextPageToken,
		Endpoints:     endpoints,
	}, nil
}

func (c *NexusEndpointClient) apiSpecToPersistenceSpec(source *nexuspb.EndpointSpec) (*persistencespb.NexusEndpointSpec, error) {
	target, err := c.apiTargetToPersistenceTarget(source.GetTarget())
	if err != nil {
		return nil, err
	}
	return &persistencespb.NexusEndpointSpec{
		Name:        source.GetName(),
		Description: source.GetDescription(),
		Target:      target,
	}, nil
}

func (c *NexusEndpointClient) apiTargetToPersistenceTarget(source *nexuspb.EndpointTarget) (*persistencespb.NexusEndpointTarget, error) {
	switch v := source.GetVariant().(type) {
	case *nexuspb.EndpointTarget_External_:
		return &persistencespb.NexusEndpointTarget{
			Variant: &persistencespb.NexusEndpointTarget_External_{
				External: &persistencespb.NexusEndpointTarget_External{
					Url: v.External.GetUrl(),
				},
			},
		}, nil
	case *nexuspb.EndpointTarget_Worker_:
		nsID, err := c.namespaceRegistry.GetNamespaceID(namespace.Name(v.Worker.GetNamespace()))
		if err != nil {
			return nil, c.transformServiceError(err, "internal error")
		}
		return &persistencespb.NexusEndpointTarget{
			Variant: &persistencespb.NexusEndpointTarget_Worker_{
				Worker: &persistencespb.NexusEndpointTarget_Worker{
					NamespaceId: nsID.String(),
					TaskQueue:   v.Worker.GetTaskQueue(),
				},
			},
		}, nil
	default:
		return nil, serviceerror.NewInvalidArgument("unknown endpoint target variant")
	}
}

// listAndFilterByName paginates over all endpoints returned by persistence layer to find the endpoint name
// indicated in the request. Returns that endpoint if found or an empty response if not.
// PageSize and NextPageToken fields on the request are ignored.
func (c *NexusEndpointClient) listAndFilterByName(
	ctx context.Context,
	request *operatorservice.ListNexusEndpointsRequest,
) (*operatorservice.ListNexusEndpointsResponse, error) {
	result := &operatorservice.ListNexusEndpointsResponse{Endpoints: []*nexuspb.Endpoint{}}
	pageSize := c.config.listDefaultPageSize()
	var currentPageToken []byte

	for ctx.Err() == nil {
		resp, err := c.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
			LastKnownTableVersion: 0,
			NextPageToken:         currentPageToken,
			PageSize:              pageSize,
		})
		if err != nil {
			c.logger.Error(fmt.Sprintf("error listing Nexus endpoints from persistence with Name filter. CurrentPageToken: %v PageSize: %d Name: %v", currentPageToken, pageSize, request.Name), tag.Error(err))
			return nil, serviceerror.NewInternal("error listing Nexus endpoints")
		}

		for _, entry := range resp.Entries {
			if request.Name == entry.Endpoint.Spec.Name {
				endpoint, err := c.endpointPersistedEntryToExternalAPI(entry)
				if err != nil {
					return nil, c.transformServiceError(err, "error listing Nexus endpoints")
				}
				result.Endpoints = []*nexuspb.Endpoint{endpoint}
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

func (c *NexusEndpointClient) getEndpointNameIssues(name string) rpc.RequestIssues {
	var issues rpc.RequestIssues

	if name == "" {
		issues.Append("endpoint name not set")
		return issues
	}

	maxNameLength := c.config.maxNameLength()
	if len(name) > maxNameLength {
		issues.Appendf("endpoint name exceeds length limit of %d", maxNameLength)
	}

	if !EndpointNameRegex.MatchString(name) {
		issues.Appendf("endpoint name must match the regex: %q", EndpointNameRegex.String())
	}

	return issues
}

func (c *NexusEndpointClient) validateUpsertSpec(spec *nexuspb.EndpointSpec) error {
	issues := c.getEndpointNameIssues(spec.GetName())
	if spec.GetTarget().GetVariant() == nil {
		issues.Append("empty target variant")
		return issues.GetError()
	}

	switch variant := spec.Target.Variant.(type) {
	case *nexuspb.EndpointTarget_Worker_:
		if variant.Worker.GetNamespace() == "" {
			issues.Append("target namespace not set")
		} else if _, nsErr := c.namespaceRegistry.GetNamespace(namespace.Name(variant.Worker.GetNamespace())); nsErr != nil {
			return serviceerror.NewFailedPreconditionf("could not verify namespace referenced by target exists: %v", nsErr.Error())
		}

		if err := tqid.Validate(variant.Worker.GetTaskQueue(), c.config.maxTaskQueueLength()); err != nil {
			issues.Appendf("invalid target task queue: %q", err.Error())
		}
	case *nexuspb.EndpointTarget_External_:
		if variant.External.GetUrl() == "" {
			issues.Append("empty target URL")
		} else if len(variant.External.GetUrl()) > c.config.maxExternalEndpointURLLength() {
			issues.Appendf("target URL length exceeds limit of %d", c.config.maxExternalEndpointURLLength())
		} else {
			u, err := url.Parse(variant.External.GetUrl())
			if err != nil {
				issues.Appendf("invalid target URL: %s", err.Error())
			} else if u.Scheme != "http" && u.Scheme != "https" {
				issues.Appendf("invalid target URL scheme: %q, expected http or https", u.Scheme)
			}
		}
	default:
		issues.Append("empty endpoint target")
	}

	maxSize := c.config.maxDescriptionSize()
	if spec.GetDescription().Size() > maxSize {
		issues.Appendf("description size exceeds limit of %d", maxSize)
	}

	return issues.GetError()
}

func getEndpointIDIssues(ID string) rpc.RequestIssues {
	var issues rpc.RequestIssues
	if ID == "" {
		issues.Append("endpoint ID not set")
	} else if _, err := uuid.Parse(ID); err != nil {
		issues.Appendf("malformed endpoint ID: %q", err.Error())
	}
	return issues
}

func validateDeleteRequest(request *operatorservice.DeleteNexusEndpointRequest) error {
	issues := getEndpointIDIssues(request.GetId())

	if request.GetVersion() <= 0 {
		issues.Append("endpoint version is non-positive")
	}

	return issues.GetError()
}

func validateGetRequest(request *operatorservice.GetNexusEndpointRequest) error {
	issues := getEndpointIDIssues(request.GetId())
	return issues.GetError()
}

func (c *NexusEndpointClient) validatePageSize(pageSize int32) error {
	// pageSize == 0 is treated as unset and will be changed to the default and does not go through this validation
	if pageSize < 0 {
		return serviceerror.NewInvalidArgument("page_size is negative")
	}

	maxPageSize := c.config.listMaxPageSize()
	if pageSize > int32(maxPageSize) {
		return serviceerror.NewInvalidArgumentf("page_size exceeds limit of %d", maxPageSize)
	}

	return nil
}

func (c *NexusEndpointClient) transformServiceError(err error, message string) error {
	if err == nil {
		return nil
	}
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return err
	}
	c.logger.Error(message, tag.Error(err))
	return serviceerror.NewInternal(message)
}

func (c *NexusEndpointClient) endpointPersistedEntryToExternalAPI(entry *persistencespb.NexusEndpointEntry) (*nexuspb.Endpoint, error) {
	persistedSpec := entry.GetEndpoint().GetSpec()
	if persistedSpec == nil {
		return nil, serviceerror.NewInternal("empty endpoint spec")
	}
	var target *nexuspb.EndpointTarget
	switch v := persistedSpec.GetTarget().GetVariant().(type) {
	case *persistencespb.NexusEndpointTarget_External_:
		target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_External_{
				External: &nexuspb.EndpointTarget_External{
					Url: v.External.Url,
				},
			},
		}
	case *persistencespb.NexusEndpointTarget_Worker_:
		name, err := c.namespaceRegistry.GetNamespaceName(namespace.ID(v.Worker.NamespaceId))
		if err != nil {
			return nil, err
		}

		target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: name.String(),
					TaskQueue: v.Worker.TaskQueue,
				},
			},
		}
	}

	var lastModifiedTime *timestamppb.Timestamp
	// Only set last modified if there were modifications as stated in the UI contract.
	if entry.Version > 1 {
		lastModifiedTime = timestamppb.New(hlc.UTC(entry.Endpoint.Clock))
	}

	spec := nexuspb.EndpointSpec{
		Name:        persistedSpec.Name,
		Description: persistedSpec.Description,
		Target:      target,
	}

	return &nexuspb.Endpoint{
		Version:          entry.Version,
		Id:               entry.Id,
		Spec:             &spec,
		CreatedTime:      entry.Endpoint.CreatedTime,
		LastModifiedTime: lastModifiedTime,
		UrlPrefix:        "/" + cnexus.RouteDispatchNexusTaskByEndpoint.Path(entry.Id),
	}, nil
}
