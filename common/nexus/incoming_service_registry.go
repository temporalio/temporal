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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
)

const (
	stopped int32 = iota
	starting
	running
	stopping
)

const (
	cacheMaxSize = 64 * 1024
	cacheTTL     = 0
)

var (
	cacheOpts = cache.Options{
		TTL: cacheTTL,
	}
)

// TODO: maybe some of this logic should live in matching_engine?
// TODO: need to add logic for determining table ownership / forwarding requests to owner
type (
	IncomingServiceRegistry interface {
		common.Pingable //TODO: necessary?
		Start()
		Stop()

		CreateOrUpdateIncomingService(ctx context.Context, request *p.CreateOrUpdateNexusIncomingServiceRequest) (*p.CreateOrUpdateNexusIncomingServiceResponse, error)
		DeleteIncomingService(ctx context.Context, request *p.DeleteNexusIncomingServiceRequest) error

		GetIncomingService(ctx context.Context, id string) (*persistencepb.VersionedNexusIncomingService, error)
		ListIncomingServices(ctx context.Context, request *p.ListNexusIncomingServicesRequest, wait bool) (*p.ListNexusIncomingServicesResponse, error)
	}

	registry struct {
		status           int32
		listPoller       *goro.Handle
		triggerRefreshCh chan *refreshParams //TODO: fix this

		sync.RWMutex    // this mutex protects both curTableVersion and cacheByID
		curTableVersion int64
		cacheByID       cache.Cache

		persistence    p.NexusServiceManager
		metricsHandler metrics.Handler
		logger         log.Logger
	}

	refreshParams struct {
		ctx     context.Context
		request *p.ListNexusIncomingServicesRequest
	}
)

func NewIncomingServiceRegistry(
	persistence p.NexusServiceManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
) IncomingServiceRegistry {
	return &registry{
		persistence:      persistence,
		metricsHandler:   metricsHandler,
		logger:           logger,
		triggerRefreshCh: make(chan *refreshParams, 1),
		cacheByID:        cache.New(cacheMaxSize, &cacheOpts),
	}
}

func (r *registry) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, stopped, starting) {
		return
	}
	defer atomic.StoreInt32(&r.status, running)

	// initialize the cache
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	err := r.refreshServices(ctx)
	if err != nil {
		r.logger.Fatal("unable to initialize nexus incoming service cache", tag.Error(err))
	}

	r.listPoller = goro.NewHandle(ctx).Go(r.pollLoop)
}

func (r *registry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, running, stopping) {
		return
	}
	defer atomic.StoreInt32(&r.status, stopped)

	r.listPoller.Cancel()
	<-r.listPoller.Done()
}

func (r *registry) GetPingChecks() []common.PingCheck {
	return []common.PingCheck{
		{
			// TODO: copied from namespace registry; probably needs some changes. include some info about table ownership here? maybe not necessary at all?
			Name: "nexus incoming service registry lock",
			// we don't do any persistence ops, this shouldn't be blocked
			Timeout: 10 * time.Second,
			Ping: func() []common.Pingable {
				r.Lock()
				//lint:ignore SA2001 just checking if we can acquire the lock
				r.Unlock()
				return nil
			},
		},
	}
}

func (r *registry) CreateOrUpdateIncomingService(
	ctx context.Context,
	request *p.CreateOrUpdateNexusIncomingServiceRequest,
) (*p.CreateOrUpdateNexusIncomingServiceResponse, error) {
	resp, err := r.persistence.CreateOrUpdateNexusIncomingService(ctx, request)
	if err != nil {
		return nil, err
	}

	request.Service.Service.Id = resp.ServiceID
	request.Service.Version = resp.Version

	r.updateCacheByID(request.Service)

	return resp, nil
}

func (r *registry) updateCacheByID(
	service *persistencepb.VersionedNexusIncomingService,
) {
	r.Lock()
	defer r.Unlock()
	r.cacheByID.Put(service.Service.Id, service)
	r.curTableVersion++
}

func (r *registry) DeleteIncomingService(
	ctx context.Context,
	request *p.DeleteNexusIncomingServiceRequest,
) error {
	err := r.persistence.DeleteNexusIncomingService(ctx, request)
	if err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()
	r.cacheByID.Delete(request.ID)
	r.curTableVersion++

	return nil
}

func (r *registry) GetIncomingService(
	ctx context.Context, // TODO: only needed if we're going to refresh on cache miss
	id string,
) (*persistencepb.VersionedNexusIncomingService, error) {
	service, err := r.getCachedIncomingService(id)
	if err == nil {
		return service, nil
	}

	// TODO: trigger refresh on not found?
	return nil, err
}

func (r *registry) getCachedIncomingService(
	id string,
) (*persistencepb.VersionedNexusIncomingService, error) {
	r.RLock()
	defer r.RUnlock()

	if service, ok := r.cacheByID.Get(id).(*persistencepb.VersionedNexusIncomingService); ok {
		return service, nil
	}
	// TODO: create constant for error
	return nil, serviceerror.NewNotFound("nexus incoming service not found")
}

func (r *registry) ListIncomingServices(
	ctx context.Context,
	request *p.ListNexusIncomingServicesRequest,
	wait bool,
) (*p.ListNexusIncomingServicesResponse, error) {
	if wait {
		r.triggerRefreshCh <- &refreshParams{
			ctx:     ctx,
			request: request,
		}
		// TODO: need to make sure refresh is completed before checking cache. handled implicitly by waiting for cache lock?
	}

	services, err := r.listAllCachedServices()
	if err != nil {
		return nil, err
	}

	return &p.ListNexusIncomingServicesResponse{
		TableVersion:  r.curTableVersion,
		NextPageToken: nil,
		Services:      services,
	}, nil
}

func (r *registry) listAllCachedServices() ([]*persistencepb.VersionedNexusIncomingService, error) {
	// TODO: how to handle pagination of cache entries? keeping iterator open probably bad idea
	// TODO: how to handle when more entries than will fit in cache?

	r.RLock()
	defer r.RUnlock()

	result := make([]*persistencepb.VersionedNexusIncomingService, r.cacheByID.Size())

	itr := r.cacheByID.Iterator()
	defer itr.Close()

	for i := range result {
		if !itr.HasNext() {
			// TODO: this check shouldn't be necessary
			break
		}
		entry := itr.Next()
		service, ok := entry.Value().(*persistencepb.VersionedNexusIncomingService)
		if !ok {
			// TODO: this check shouldn't be necessary
			return nil, serviceerror.NewInternal("error iterating cached nexus incoming service list")
		}
		result[i] = service
	}

	return result, nil
}

func (r *registry) pollLoop(ctx context.Context) error {
	// TODO: implement me
	return nil
}

func (r *registry) refreshServices(ctx context.Context) error {
	// TODO: implement me
	return nil
}

func (r *registry) listServicesFromPersistence(
	ctx context.Context,
	request *p.ListNexusIncomingServicesRequest,
) (*p.ListNexusIncomingServicesResponse, error) {
	return r.persistence.ListNexusIncomingServices(ctx, request)
}
