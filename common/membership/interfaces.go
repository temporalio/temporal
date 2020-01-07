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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interfaces_mock.go -self_package github.com/uber/cadence/common/membership

package membership

import (
	"errors"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// ErrUnknownService is thrown for a service that is not tracked by this instance
var ErrUnknownService = errors.New("Service not tracked by Monitor")

// ErrInsufficientHosts is thrown when there are not enough hosts to serve the request
var ErrInsufficientHosts = &shared.InternalServiceError{Message: "Not enough hosts to serve the request"}

// ErrListenerAlreadyExist is thrown on a duplicate AddListener call from the same listener
var ErrListenerAlreadyExist = errors.New("Listener already exist for the service")

type (

	// ChangedEvent describes a change in membership
	ChangedEvent struct {
		HostsAdded   []*HostInfo
		HostsUpdated []*HostInfo
		HostsRemoved []*HostInfo
	}

	// Monitor provides membership information for all cadence services.
	// It can be used to query which member host of a service is responsible for serving a given key.
	Monitor interface {
		common.Daemon

		WhoAmI() (*HostInfo, error)
		Lookup(service string, key string) (*HostInfo, error)
		GetResolver(service string) (ServiceResolver, error)
		// AddListener adds a listener for this service.
		// The listener will get notified on the given
		// channel, whenever there is a membership change.
		// @service: The service to be listened on
		// @name: The name for identifying the listener
		// @notifyChannel: The channel on which the caller receives notifications
		AddListener(service string, name string, notifyChannel chan<- *ChangedEvent) error
		// RemoveListener removes a listener for this service.
		RemoveListener(service string, name string) error
		// GetReachableMembers returns addresses of all members of the ring
		GetReachableMembers() ([]string, error)
	}

	// ServiceResolver provides membership information for a specific cadence service.
	// It can be used to resolve which member host is responsible for serving a given key.
	ServiceResolver interface {
		Lookup(key string) (*HostInfo, error)
		// AddListener adds a listener which will get notified on the given
		// channel, whenever membership changes.
		// @name: The name for identifying the listener
		// @notifyChannel: The channel on which the caller receives notifications
		AddListener(name string, notifyChannel chan<- *ChangedEvent) error
		// RemoveListener removes a listener for this service.
		RemoveListener(name string) error
		// MemberCount returns host count in hashring for any particular role
		MemberCount() int
		// Members returns all host addresses in hashring for any particular role
		Members() []*HostInfo
	}
)
