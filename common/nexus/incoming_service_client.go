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
	"context"
	"errors"
	"sync/atomic"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/internal/goro"
)

const (
	// tableOwnershipPartitionKey is a dummy namespace name that is used to route matching requests to the owner of the nexus_incoming_services table.
	// If it is updated, be sure to also update client generation in cmd/tools/rpcwrappers/main.go
	//TODO: not sure if this will work
	tableOwnershipPartitionKey                            = "_system_nexus_incoming_services_owner"
	nexusIncomingServicesMatchingMembershipUpdateListener = "NexusIncomingServicesClient"
)

type (
	// incomingServiceClient handles routing of Nexus incoming service CRUD requests to the matching node
	// that owns the nexus_incoming_services table.
	incomingServiceClient struct {
		isTableOwner atomic.Bool

		serviceName              primitives.ServiceName
		hostInfoProvider         membership.HostInfoProvider
		matchingServiceResolver  membership.ServiceResolver
		membershipUpdateCh       chan *membership.ChangedEvent
		ownershipChangedListener *goro.Handle

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusServiceManager

		logger log.Logger
	}
)

func newNexusIncomingServiceClient(
	serviceName primitives.ServiceName,
	hostInfoProvider membership.HostInfoProvider,
	matchingServiceResolver membership.ServiceResolver,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusServiceManager,
	logger log.Logger,
) *incomingServiceClient {
	return &incomingServiceClient{
		serviceName:             serviceName,
		hostInfoProvider:        hostInfoProvider,
		matchingServiceResolver: matchingServiceResolver,
		membershipUpdateCh:      make(chan *membership.ChangedEvent, 1),
		matchingClient:          matchingClient,
		persistence:             persistence,
		logger:                  logger,
	}
}

func (c *incomingServiceClient) Start() {
	if c.serviceName == primitives.MatchingService {
		if err := c.matchingServiceResolver.AddListener(
			nexusIncomingServicesMatchingMembershipUpdateListener,
			c.membershipUpdateCh,
		); err != nil {
			c.logger.Fatal("error adding nexus incoming service client matching membership listener", tag.Error(err))
		}

		ctx := headers.SetCallerInfo(
			context.Background(),
			headers.SystemBackgroundCallerInfo,
		)
		c.ownershipChangedListener = goro.NewHandle(ctx).Go(c.membershipChangeListenLoop)
	}
}

func (c *incomingServiceClient) Stop() {
	c.persistence.Close()
	if c.serviceName == primitives.MatchingService {
		if err := c.matchingServiceResolver.RemoveListener(
			nexusIncomingServicesMatchingMembershipUpdateListener,
		); err != nil {
			c.logger.Error("error removing nexus incoming service client matching membership listener", tag.Error(err))
		}

		c.ownershipChangedListener.Cancel()
		<-c.ownershipChangedListener.Done()
	}
}

func (c *incomingServiceClient) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *p.CreateOrUpdateNexusIncomingServiceRequest,
) (*persistencepb.VersionedNexusIncomingService, error) {
	if !c.isTableOwner.Load() {
		resp, err := c.matchingClient.CreateOrUpdateNexusService(ctx, &matchingservice.CreateOrUpdateNexusServiceRequest{
			Service: request.Service,
		})

		if err == nil {
			return resp.Service, nil
		} else if isRetryableClientError(err) {
			// Only return ConditionFailed / transient errors. Otherwise, fallback to calling persistence directly.
			// TODO: verify this is desired behavior
			return nil, err
		}
	}

	resp, err := c.persistence.CreateOrUpdateNexusIncomingService(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp.Service, err
}

func (c *incomingServiceClient) DeleteNexusIncomingService(
	ctx context.Context,
	lastKnownTableVersion int64,
	serviceID string,
	serviceName string,
) error {
	if !c.isTableOwner.Load() {
		_, err := c.matchingClient.DeleteNexusService(ctx, &matchingservice.DeleteNexusServiceRequest{Name: serviceName})
		if isRetryableClientError(err) {
			// Only return ConditionFailed / transient errors. Otherwise, fallback to calling persistence directly.
			// TODO: verify this is desired behavior
			return err
		}
	}

	return c.persistence.DeleteNexusIncomingService(ctx, &p.DeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: lastKnownTableVersion,
		ServiceID:             serviceID,
	})
}

func (c *incomingServiceClient) ListNexusIncomingServices(
	ctx context.Context,
	request *matchingservice.ListNexusServicesRequest,
) (*p.ListNexusIncomingServicesResponse, error) {
	result := &p.ListNexusIncomingServicesResponse{}
	var err error
	if !c.isTableOwner.Load() {
		var resp *matchingservice.ListNexusServicesResponse
		resp, err = c.matchingClient.ListNexusServices(ctx, request)
		if resp != nil {
			result.TableVersion = resp.TableVersion
			result.NextPageToken = resp.NextPageToken
			result.Services = resp.Services
		}
		if isRetryableClientError(err) {
			// Only return ConditionFailed / transient errors. Otherwise, fallback to calling persistence directly.
			// TODO: verify this is desired behavior
			return result, err
		}
	}

	if request.Wait {
		// Cannot long poll on a persistence request, so return immediately if one was requested.
		// TODO: should probably just check table version (i.e. page size 0)
		// TODO: verify this is desired behavior
		return result, err
	}
	return c.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		NextPageToken:         request.NextPageToken,
		PageSize:              int(request.PageSize),
	})
}

func (c *incomingServiceClient) membershipChangeListenLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.membershipUpdateCh:
			ownerInfo, err := c.matchingServiceResolver.Lookup(tableOwnershipPartitionKey)
			if err != nil {
				return err //TODO: error handling
			}

			c.isTableOwner.Store(ownerInfo.Identity() == c.hostInfoProvider.HostInfo().Identity())
		}
	}
}

func isRetryableClientError(err error) bool {
	if err == nil {
		return false
	}
	if common.IsServiceClientTransientError(err) {
		return true
	}
	var conditionFailedError *p.ConditionFailedError
	return errors.As(err, &conditionFailedError)
}
