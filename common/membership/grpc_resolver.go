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

package membership

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/fx"
	"google.golang.org/grpc/resolver"

	"go.temporal.io/server/common/primitives"
)

// GRPCResolver is an empty type used to enforce a dependency using fx so that we're guaranteed to have initialized
// the global builder before we use it.
type GRPCResolver struct{}

var (
	GRPCResolverModule = fx.Options(
		fx.Provide(initializeBuilder),
	)

	globalGrpcBuilder grpcBuilder
)

func init() {
	// This must be called in init to avoid race conditions. We don't have a Monitor yet, so we'll leave it nil and
	// initialize it with fx.
	resolver.Register(&globalGrpcBuilder)
}

func initializeBuilder(monitor Monitor) GRPCResolver {
	globalGrpcBuilder.monitor.Store(monitor)
	return GRPCResolver{}
}

type grpcBuilder struct {
	monitor atomic.Value // Monitor
}

func (m *grpcBuilder) Scheme() string {
	return ResolverScheme
}

func (m *grpcBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	monitor, ok := m.monitor.Load().(Monitor)
	if !ok {
		return nil, errors.New("grpc resolver has not been initialized yet")
	}
	// See MakeURL: the service ends up as the "host" of the parsed URL
	service := target.URL.Host
	serviceResolver, err := monitor.GetResolver(primitives.ServiceName(service))
	if err != nil {
		return nil, err
	}
	grpcResolver := &grpcResolver{
		cc:       cc,
		r:        serviceResolver,
		notifyCh: make(chan *ChangedEvent, 1),
	}
	if err := grpcResolver.start(); err != nil {
		return nil, err
	}
	return grpcResolver, nil
}

type grpcResolver struct {
	cc       resolver.ClientConn
	r        ServiceResolver
	notifyCh chan *ChangedEvent
	wg       sync.WaitGroup
}

func (m *grpcResolver) start() error {
	if err := m.r.AddListener(fmt.Sprintf("%p", m), m.notifyCh); err != nil {
		return err
	}
	m.wg.Add(1)
	go m.listen()

	// Try once to get address synchronously. If this fails, it's okay, we'll listen for
	// changes and update the resolver later.
	m.resolve()
	return nil
}

func (m *grpcResolver) listen() {
	for range m.notifyCh {
		m.resolve()
	}
	m.wg.Done()
}

func (m *grpcResolver) resolve() {
	members := m.r.AvailableMembers()
	if len(members) == 0 {
		// grpc considers it an error if we report no addresses, and fails the connection eagerly.
		// Instead, just poke membership and then wait until it notifies us.
		m.r.RequestRefresh()
		return
	}
	addresses := make([]resolver.Address, 0, len(members))
	for _, hostInfo := range members {
		addresses = append(addresses, resolver.Address{
			Addr: hostInfo.GetAddress(),
		})
	}
	if err := m.cc.UpdateState(resolver.State{Addresses: addresses}); err != nil {
		fmt.Printf("error updating state in gRPC resolver: %v", err)
	}
}

func (m *grpcResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	select {
	case m.notifyCh <- nil:
	default:
	}
}

func (m *grpcResolver) Close() {
	if err := m.r.RemoveListener(fmt.Sprintf("%p", m)); err != nil {
		fmt.Printf("error removing listener from gRPC resolver: %v", err)
	}
	close(m.notifyCh)
	m.wg.Wait() // wait until listen() exits
}
