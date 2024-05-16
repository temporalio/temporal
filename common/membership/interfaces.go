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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interfaces_mock.go

package membership

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/primitives"
)

// ErrUnknownService is thrown for a service that is not tracked by this instance
var ErrUnknownService = errors.New("service not tracked by Monitor")

// ErrInsufficientHosts is thrown when there are not enough hosts to serve the request
var ErrInsufficientHosts = serviceerror.NewUnavailable("Not enough hosts to serve the request")

// ErrListenerAlreadyExist is thrown on a duplicate AddListener call from the same listener
var ErrListenerAlreadyExist = errors.New("listener already exist for the service")

// ErrIncorrectAddressFormat is thrown on incorrect address format
var ErrIncorrectAddressFormat = errors.New("incorrect address format")

type (
	// ChangedEvent describes a change in membership
	ChangedEvent struct {
		HostsAdded   []HostInfo
		HostsRemoved []HostInfo
		HostsChanged []HostInfo
	}

	// Monitor provides membership information for all temporal services.
	// It can be used to query which member host of a service is responsible for serving a given key.
	Monitor interface {
		// Start causes this service to join the membership ring. Services
		// should not call Start until they are ready to receive requests from
		// other cluster members.
		Start()
		// EvictSelf evicts this member from the membership ring. After this method is
		// called, other members will discover that this node is no longer part of the
		// ring. This primitive is useful to carry out graceful host shutdown during deployments.
		EvictSelf() error
		// EvictSelfAt is similar to EvictSelf but causes the change to take effect on all
		// hosts at that absolute time (assuming it's in the future). This process should stay
		// alive for at least the returned duration after calling this, so that all membership
		// information can be propagated correctly. The resolution of asOf is whole seconds.
		EvictSelfAt(asOf time.Time) (time.Duration, error)
		// GetResolver returns the service resolver for a service in the cluster.
		GetResolver(service primitives.ServiceName) (ServiceResolver, error)
		// GetReachableMembers returns addresses of all members of the ring.
		GetReachableMembers() ([]string, error)
		// WaitUntilInitialized blocks until initialization is completed and returns the result
		// of initialization. The current implementation does log.Fatal if it can't initialize,
		// so currently this will never return non-nil, except for context cancel/timeout. A
		// future implementation might return more errors.
		WaitUntilInitialized(context.Context) error
		// SetDraining sets the draining state (synchronized through ringpop)
		SetDraining(draining bool) error
		// ApproximateMaxPropagationTime returns an approximate upper bound on propagation time
		// for updates to membership information. This is _not_ a guarantee! This value is only
		// provided to help with startup/shutdown timing as a best-effort.
		ApproximateMaxPropagationTime() time.Duration
	}

	// ServiceResolver provides membership information for a specific temporal service.
	// It can also be used to determine the placement of resources across hosts.
	ServiceResolver interface {
		// Lookup looks up the host that currently owns the resource identified by the given key.
		Lookup(key string) (HostInfo, error)
		// LookupN looks n hosts that owns the resource identified by the given key, if n greater than total number
		// of hosts total number of hosts will be returned
		LookupN(key string, n int) []HostInfo
		// AddListener adds a listener which will get notified on the given channel whenever membership changes.
		AddListener(name string, notifyChannel chan<- *ChangedEvent) error
		// RemoveListener removes a listener for this service.
		RemoveListener(name string) error
		// MemberCount returns the number of known hosts running this service.
		MemberCount() int
		// AvailableMemberCount returns the number of hosts running this service that are accepting requests (not draining).
		AvailableMemberCount() int
		// Members returns all known hosts available for this service.
		Members() []HostInfo
		// AvailableMembers returns all hosts available for this service that are accepting requests (not draining).
		AvailableMembers() []HostInfo
		// RequestRefresh requests that the membership information be refreshed.
		RequestRefresh()
	}

	HostInfoProvider interface {
		HostInfo() HostInfo
	}
)
