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
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/primitives"

	"github.com/pborman/uuid"

	"go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	upsertMembershipRecordExpiryDefault = time.Hour * 48

	// 10 second base reporting frequency + 5 second jitter + 5 second acceptable time skew
	healthyHostLastHeartbeatCutoff = time.Second * 20
)

type ringpopMonitor struct {
	status int32

	serviceName               string
	services                  map[string]int
	rp                        *RingPop
	rings                     map[string]*ringpopServiceResolver
	logger                    log.Logger
	metadataManager           persistence.ClusterMetadataManager
	broadcastHostPortResolver func() (string, error)
	hostID                    uuid.UUID
}

var _ Monitor = (*ringpopMonitor)(nil)

// NewRingpopMonitor returns a ringpop-based membership monitor
func NewRingpopMonitor(
	serviceName string,
	services map[string]int,
	rp *RingPop,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
	broadcastHostPortResolver func() (string, error),
) Monitor {

	rpo := &ringpopMonitor{
		broadcastHostPortResolver: broadcastHostPortResolver,
		metadataManager:           metadataManager,
		status:                    common.DaemonStatusInitialized,
		serviceName:               serviceName,
		services:                  services,
		rp:                        rp,
		logger:                    logger,
		rings:                     make(map[string]*ringpopServiceResolver),
		hostID:                    uuid.NewUUID(),
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

	broadcastAddress, err := rpo.broadcastHostPortResolver()
	if err != nil {
		rpo.logger.Fatal("unable to resolve broadcast address", tag.Error(err))
	}

	// TODO - Note this presents a small race condition as we write our identity before we bootstrap ringpop.
	// This is a current limitation of the current structure of the ringpop library as
	// we must know our seed nodes before bootstrapping
	err = rpo.startHeartbeat(broadcastAddress)
	if err != nil {
		rpo.logger.Fatal("unable to initialize membership heartbeats", tag.Error(err))
	}

	rpo.rp.Start(
		func() ([]string, error) { return fetchCurrentBootstrapHostports(rpo.metadataManager, rpo.logger) },
		healthyHostLastHeartbeatCutoff/2)

	labels, err := rpo.rp.Labels()
	if err != nil {
		rpo.logger.Fatal("unable to get ring pop labels", tag.Error(err))
	}

	if err = labels.Set(RolePort, strconv.Itoa(rpo.services[rpo.serviceName])); err != nil {
		rpo.logger.Fatal("unable to set ring pop ServicePort label", tag.Error(err))
	}

	if err = labels.Set(RoleKey, rpo.serviceName); err != nil {
		rpo.logger.Fatal("unable to set ring pop ServiceRole label", tag.Error(err))
	}

	for _, ring := range rpo.rings {
		ring.Start()
	}
}

func ServiceNameToServiceTypeEnum(name string) (persistence.ServiceType, error) {
	switch name {
	case primitives.AllServices:
		return persistence.All, nil
	case primitives.FrontendService:
		return persistence.Frontend, nil
	case primitives.HistoryService:
		return persistence.History, nil
	case primitives.MatchingService:
		return persistence.Matching, nil
	case primitives.WorkerService:
		return persistence.Worker, nil
	default:
		return persistence.All, fmt.Errorf("unable to parse servicename '%s'", name)
	}
}

func (rpo *ringpopMonitor) upsertMyMembership(
	ctx context.Context,
	request *persistence.UpsertClusterMembershipRequest,
) error {
	err := rpo.metadataManager.UpsertClusterMembership(ctx, request)

	if err == nil {
		rpo.logger.Debug("Membership heartbeat upserted successfully",
			tag.Address(request.RPCAddress.String()),
			tag.Port(int(request.RPCPort)),
			tag.HostID(request.HostID.String()))
	}

	return err
}

// SplitHostPortTyped expands upon net.SplitHostPort by providing type parsing.
func SplitHostPortTyped(hostPort string) (net.IP, uint16, error) {
	ipstr, portstr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, 0, err
	}

	broadcastAddress := net.ParseIP(ipstr)
	broadcastPort, err := strconv.ParseUint(portstr, 10, 16)
	if err != nil {
		return nil, 0, err
	}

	return broadcastAddress, uint16(broadcastPort), nil
}

func (rpo *ringpopMonitor) startHeartbeat(broadcastHostport string) error {
	// Start by cleaning up expired records to avoid growth
	err := rpo.metadataManager.PruneClusterMembership(context.TODO(), &persistence.PruneClusterMembershipRequest{MaxRecordsPruned: 10})
	if err != nil {
		return err
	}

	sessionStarted := time.Now().UTC()

	// Parse and validate broadcast hostport
	broadcastAddress, broadcastPort, err := SplitHostPortTyped(broadcastHostport)
	if err != nil {
		return err
	}

	// Parse and validate existing service name
	role, err := ServiceNameToServiceTypeEnum(rpo.serviceName)
	if err != nil {
		return err
	}

	req := &persistence.UpsertClusterMembershipRequest{
		Role:         role,
		RPCAddress:   broadcastAddress,
		RPCPort:      broadcastPort,
		SessionStart: sessionStarted,
		RecordExpiry: upsertMembershipRecordExpiryDefault,
		HostID:       rpo.hostID,
	}

	// Upsert before fetching bootstrap hosts.
	// This makes us discoverable by other Temporal cluster members
	// Expire in 48 hours to allow for inspection of table by humans for debug scenarios.
	// For bootstrapping, we filter to a much shorter duration on the
	// read side by filtering on the last time a heartbeat was seen.
	err = rpo.upsertMyMembership(context.TODO(), req)
	if err == nil {
		rpo.logger.Info("Membership heartbeat upserted successfully",
			tag.Address(broadcastAddress.String()),
			tag.Port(int(broadcastPort)),
			tag.HostID(rpo.hostID.String()))

		rpo.startHeartbeatUpsertLoop(req)
	}

	return err
}

func fetchCurrentBootstrapHostports(manager persistence.ClusterMetadataManager, log log.Logger) ([]string, error) {
	pageSize := 1000
	set := make(map[string]struct{})

	var nextPageToken []byte

	for {
		resp, err := manager.GetClusterMembers(
			context.TODO(),
			&persistence.GetClusterMembersRequest{
				LastHeartbeatWithin: healthyHostLastHeartbeatCutoff,
				PageSize:            pageSize,
				NextPageToken:       nextPageToken,
			})

		if err != nil {
			return nil, err
		}

		// Dedupe on hostport
		for _, host := range resp.ActiveMembers {
			set[net.JoinHostPort(host.RPCAddress.String(), convert.Uint16ToString(host.RPCPort))] = struct{}{}
		}

		// Stop iterating once we have either 500 unique ip:port combos or there is no more results.
		if nextPageToken == nil || len(set) >= 500 {
			bootstrapHostPorts := make([]string, 0, len(set))
			for k := range set {
				bootstrapHostPorts = append(bootstrapHostPorts, k)
			}

			log.Info("bootstrap hosts fetched", tag.BootstrapHostPorts(strings.Join(bootstrapHostPorts, ",")))
			return bootstrapHostPorts, nil
		}

	}
}

func (rpo *ringpopMonitor) startHeartbeatUpsertLoop(request *persistence.UpsertClusterMembershipRequest) {
	loopUpsertMembership := func() {
		for {
			err := rpo.upsertMyMembership(context.TODO(), request)

			if err != nil {
				rpo.logger.Error("Membership upsert failed.", tag.Error(err))
			}

			jitter := math.Round(rand.Float64() * 5)
			time.Sleep(time.Second * time.Duration(10+jitter))
		}
	}

	go loopUpsertMembership()
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

func (rpo *ringpopMonitor) EvictSelf() error {
	return rpo.rp.SelfEvict()
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

func (rpo *ringpopMonitor) GetReachableMembers() ([]string, error) {
	return rpo.rp.GetReachableMembers()
}

func (rpo *ringpopMonitor) GetMemberCount(service string) (int, error) {
	ring, err := rpo.GetResolver(service)
	if err != nil {
		return 0, err
	}
	return ring.MemberCount(), nil
}

func replaceServicePort(address string, servicePort int) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", ErrIncorrectAddressFormat
	}
	return net.JoinHostPort(host, convert.IntToString(servicePort)), nil
}
