// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

type ringpopMonitor struct {
	status int32

	serviceName string
	services    map[string]int
	rp          *RingPop
	rings       map[string]*ringpopServiceResolver
	logger      log.Logger
}

var _ Monitor = (*ringpopMonitor)(nil)

// NewRingpopMonitor returns a ringpop-based membership monitor
func NewRingpopMonitor(
	serviceName string,
	services map[string]int,
	rp *RingPop,
	logger log.Logger,
) Monitor {

	rpo := &ringpopMonitor{
		status:      common.DaemonStatusInitialized,
		serviceName: serviceName,
		services:    services,
		rp:          rp,
		logger:      logger,
		rings:       make(map[string]*ringpopServiceResolver),
	}
	for service, port := range services {
		rpo.rings[service] = newRingpopServiceResolver(service, port, rp, logger)
	}
	return rpo
}

func (rpo *ringpopMonitor) Start() {
	if !atomic.CompareAndSwapInt32(
		&rpo.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	rpo.rp.Start()

	labels, err := rpo.rp.Labels()
	if err != nil {
		rpo.logger.Fatal("unable to get ring pop labels", tag.Error(err))
	}

	if err = labels.Set(RoleKey, rpo.serviceName); err != nil {
		rpo.logger.Fatal("unable to set ring pop labels", tag.Error(err))
	}

	for _, ring := range rpo.rings {
		ring.Start()
	}
}

func (rpo *ringpopMonitor) Stop() {
	if !atomic.CompareAndSwapInt32(
		&rpo.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	for _, ring := range rpo.rings {
		ring.Stop()
	}

	rpo.rp.Stop()
}

// WhoAmI returns the address (host:port) and labels for a service
// Ringpop implementation of WhoAmI return the address used by ringpop listener.
// This is different from service address as we register ringpop handlers on a separate port.
// For this reason we need to lookup the port for the service and replace ringpop port with service port before
// returning HostInfo back.
func (rpo *ringpopMonitor) WhoAmI() (*HostInfo, error) {
	address, err := rpo.rp.WhoAmI()
	if err != nil {
		return nil, err
	}
	labels, err := rpo.rp.Labels()
	if err != nil {
		return nil, err
	}

	servicePort, ok := rpo.services[rpo.serviceName]
	if !ok {
		return nil, ErrUnknownService
	}

	serviceAddress, err := replaceServicePort(address, servicePort)
	if err != nil {
		return nil, err
	}
	return NewHostInfo(serviceAddress, labels.AsMap()), nil
}

func (rpo *ringpopMonitor) GetResolver(service string) (ServiceResolver, error) {
	ring, found := rpo.rings[service]
	if !found {
		return nil, ErrUnknownService
	}
	return ring, nil
}

func (rpo *ringpopMonitor) Lookup(service string, key string) (*HostInfo, error) {
	ring, err := rpo.GetResolver(service)
	if err != nil {
		return nil, err
	}
	return ring.Lookup(key)
}

func (rpo *ringpopMonitor) AddListener(service string, name string, notifyChannel chan<- *ChangedEvent) error {
	ring, err := rpo.GetResolver(service)
	if err != nil {
		return err
	}
	return ring.AddListener(name, notifyChannel)
}

func (rpo *ringpopMonitor) RemoveListener(service string, name string) error {
	ring, err := rpo.GetResolver(service)
	if err != nil {
		return err
	}
	return ring.RemoveListener(name)
}

func replaceServicePort(address string, servicePort int) (string, error) {
	parts := strings.Split(address, ":")
	if len(parts) != 2 {
		return "", ErrIncorrectAddressFormat
	}

	return fmt.Sprintf("%s:%v", parts[0], servicePort), nil
}
