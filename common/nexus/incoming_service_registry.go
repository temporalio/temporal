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

	"go.temporal.io/api/nexus/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
)

const (
	stopped int32 = iota
	starting
	running
	stopping
)

const (
	cacheMaxSize = 64 * 1024
	cacheTTL     = 0 // 0 means infinity
)

var (
	cacheOpts = &cache.Options{
		TTL: cacheTTL,
	}
)

type (
	IncomingServiceRegistry interface {
		Start()
		Stop()
		CreateOrUpdateService(ctx context.Context, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error)
		DeleteService(ctx context.Context, name string) error
		GetService(name string) (*persistencespb.VersionedNexusIncomingService, error)
		GetServiceID(name string) (string, error)
		ListServices(ctx context.Context, request *p.ListNexusIncomingServicesRequest) (*p.ListNexusIncomingServicesResponse, chan struct{}, error)
	}

	registry struct {
		status int32

		sync.RWMutex    // protects curTableVersion, serviceNameToID, and servicesByID
		curTableVersion int64
		serviceNameToID cache.Cache
		servicesByID    cache.Cache

		tableVersionChanged chan struct{}

		persistence    p.NexusServiceManager
		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

func NewIncomingServiceRegistry(
	persistence p.NexusServiceManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
) IncomingServiceRegistry {
	return &registry{
		persistence:         persistence,
		metricsHandler:      metricsHandler,
		logger:              logger,
		serviceNameToID:     cache.New(cacheMaxSize, cacheOpts),
		servicesByID:        cache.New(cacheMaxSize, cacheOpts),
		tableVersionChanged: make(chan struct{}),
	}
}

func (r *registry) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, stopped, starting) {
		return
	}
	defer atomic.StoreInt32(&r.status, running)

	// initialize the cache by initial scan
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	err := r.loadServices(ctx)
	if err != nil {
		r.logger.Fatal("unable to initialize Nexus incoming service registry cache", tag.Error(err))
	}
}

func (r *registry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, running, stopping) {
		return
	}
	defer atomic.StoreInt32(&r.status, stopped)
}

func (r *registry) loadServices(ctx context.Context) error {
	resp, err := r.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: 0,
	})
	if err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()

	r.curTableVersion = resp.TableVersion
	for _, service := range resp.Services {
		r.servicesByID.Put(service.Service.Id, service)
		r.serviceNameToID.Put(service.Service.Data.Name, service.Service.Id)
	}

	return nil
}

func (r *registry) CreateOrUpdateService(ctx context.Context, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error) {
	r.Lock()
	defer r.Unlock()

	resp, err := r.persistence.CreateOrUpdateNexusIncomingService(ctx, &p.CreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: r.curTableVersion,
		Service:               service,
	})
	if err != nil {
		return nil, err
	}

	r.curTableVersion++
	r.servicesByID.Put(resp.Service.Service.Id, resp.Service)
	r.serviceNameToID.Put(resp.Service.Service.Id, resp.Service.Service.Data.Name)
	close(r.tableVersionChanged)
	r.tableVersionChanged = make(chan struct{})

	return resp.Service, err
}

func (r *registry) DeleteService(ctx context.Context, name string) error {
	r.Lock()
	defer r.Unlock()

	serviceID, ok := r.serviceNameToID.Get(name).(string)
	if !ok {
		return nil //TODO: error handling
	}

	err := r.persistence.DeleteNexusIncomingService(ctx, &p.DeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: r.curTableVersion,
		ServiceID:             serviceID,
	})
	if err != nil {
		return err
	}

	r.curTableVersion++
	r.servicesByID.Delete(serviceID)
	r.serviceNameToID.Delete(name)
	close(r.tableVersionChanged)
	r.tableVersionChanged = make(chan struct{})

	return nil
}

func (r *registry) GetService(name string) (*persistencespb.VersionedNexusIncomingService, error) {
	r.RLock()
	defer r.RUnlock()

	serviceID, ok := r.serviceNameToID.Get(name).(string)
	if !ok {
		return nil, nil // TODO: error handling
	}

	service, ok := r.servicesByID.Get(serviceID).(*persistencespb.VersionedNexusIncomingService)
	if !ok {
		return nil, nil // TODO: error handling
	}

	return service, nil
}

func (r *registry) GetServiceID(name string) (string, error) {
	r.RLock()
	defer r.RUnlock()

	id, ok := r.serviceNameToID.Get(name).(string)
	if !ok {
		return "", nil // TODO: error handling
	}

	return id, nil
}

func (r *registry) ListServices(ctx context.Context, request *p.ListNexusIncomingServicesRequest) (*p.ListNexusIncomingServicesResponse, chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()

	services, err := r.listServicesLocked()
	if err != nil {
		return nil, nil, err
	}

	resp := &p.ListNexusIncomingServicesResponse{
		TableVersion: r.curTableVersion,
		Services:     services,
	}

	return resp, r.tableVersionChanged, nil
}

func (r *registry) listServicesLocked() ([]*persistencespb.VersionedNexusIncomingService, error) {
	// TODO: how to handle pagination when reading from cache

	services := make([]*persistencespb.VersionedNexusIncomingService, r.servicesByID.Size())

	itr := r.servicesByID.Iterator()
	defer itr.Close()

	for i := range services {
		entry := itr.Next()
		service, ok := entry.Value().(*persistencespb.VersionedNexusIncomingService)
		if !ok {
			return nil, nil // TODO: error handling
		}
		services[i] = service
	}

	return services, nil
}
