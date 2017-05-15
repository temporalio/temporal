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
	"sync"

	"github.com/uber-common/bark"
	ringpop "github.com/uber/ringpop-go"
)

type ringpopMonitor struct {
	started  bool
	stopped  bool
	services []string
	rp       *ringpop.Ringpop
	rings    map[string]*ringpopServiceResolver
	logger   bark.Logger
	mutex    sync.Mutex
}

var _ Monitor = (*ringpopMonitor)(nil)

// NewRingpopMonitor returns a ringpop-based membership monitor
func NewRingpopMonitor(services []string, rp *ringpop.Ringpop, logger bark.Logger) Monitor {
	rpo := &ringpopMonitor{
		services: services,
		rp:       rp,
		logger:   logger,
		rings:    make(map[string]*ringpopServiceResolver),
	}
	for _, service := range services {
		rpo.rings[service] = newRingpopServiceResolver(service, rp, logger)
	}
	return rpo
}

func (rpo *ringpopMonitor) Start() error {
	rpo.mutex.Lock()
	defer rpo.mutex.Unlock()

	if rpo.started {
		return nil
	}

	for service, ring := range rpo.rings {
		err := ring.Start()
		if err != nil {
			rpo.logger.WithField("service", service).Error("Failed to initialize ring.")
			return err
		}
	}

	rpo.started = true
	return nil
}

func (rpo *ringpopMonitor) Stop() {
	rpo.mutex.Lock()
	defer rpo.mutex.Unlock()

	if rpo.stopped {
		return
	}

	for _, ring := range rpo.rings {
		ring.Stop()
	}
	rpo.stopped = true
}

func (rpo *ringpopMonitor) WhoAmI() (*HostInfo, error) {
	address, err := rpo.rp.WhoAmI()
	if err != nil {
		return nil, err
	}
	labels, err := rpo.rp.Labels()
	if err != nil {
		return nil, err
	}
	return NewHostInfo(address, labels.AsMap()), nil
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
