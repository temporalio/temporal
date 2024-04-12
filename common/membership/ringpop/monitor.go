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

package ringpop

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/ringpop-go/discovery/statichosts"
	"github.com/temporalio/ringpop-go/swim"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

const (
	upsertMembershipRecordExpiryDefault = time.Hour * 48

	// 10 second base reporting frequency + 5 second jitter + 5 second acceptable time skew
	healthyHostLastHeartbeatCutoff = time.Second * 20

	// Number of times we retry refreshing the bootstrap list and try to join the Ringpop cluster before giving up
	maxBootstrapRetries = 5

	// We're passing around absolute timestamps, so we have to worry about clock skew.
	// With significant clock skew and scheduled start/leave times, nodes might disagree on who
	// is in the ring. Note that both joining (startAt) and leaving (stopAt) labels will only
	// be present on any node for a limited amount of time, so the duration of disagreement is
	// bounded. To be totally sure, monitor enforces a hard limit on how far in the future it
	// allows join/leave times to be. 15 seconds is enough to allow aligning to 10 seconds plus
	// 3 seconds of propagation time buffer.
	maxScheduledEventTimeSeconds = 15
)

type monitor struct {
	stateLock sync.Mutex
	status    int32

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	serviceName               primitives.ServiceName
	services                  config.ServicePortMap
	rp                        *ringpop.Ringpop
	maxJoinDuration           time.Duration
	propagationTime           time.Duration
	joinTime                  time.Time
	rings                     map[primitives.ServiceName]*serviceResolver
	logger                    log.Logger
	metadataManager           persistence.ClusterMetadataManager
	broadcastHostPortResolver func() (string, error)
	hostID                    uuid.UUID
	initialized               *future.FutureImpl[struct{}]
}

var _ membership.Monitor = (*monitor)(nil)

// newMonitor returns a ringpop-based membership monitor
func newMonitor(
	serviceName primitives.ServiceName,
	services config.ServicePortMap,
	rp *ringpop.Ringpop,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
	broadcastHostPortResolver func() (string, error),
	maxJoinDuration time.Duration,
	propagationTime time.Duration,
	joinTime time.Time,
) *monitor {
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCtx = headers.SetCallerInfo(
		lifecycleCtx,
		headers.SystemBackgroundCallerInfo,
	)

	rpo := &monitor{
		status: common.DaemonStatusInitialized,

		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,

		serviceName:               serviceName,
		services:                  services,
		rp:                        rp,
		rings:                     make(map[primitives.ServiceName]*serviceResolver),
		logger:                    logger,
		metadataManager:           metadataManager,
		broadcastHostPortResolver: broadcastHostPortResolver,
		hostID:                    uuid.NewUUID(),
		initialized:               future.NewFuture[struct{}](),
		maxJoinDuration:           maxJoinDuration,
		propagationTime:           propagationTime,
		joinTime:                  joinTime,
	}
	for service, port := range services {
		rpo.rings[service] = newServiceResolver(service, port, rp, logger)
	}
	return rpo
}

// Start the membership monitor. Stop() can be called concurrently so we relinquish the state lock when
// it's safe for Stop() to run, which is at any point when we are neither updating the status field nor
// starting rings
func (rpo *monitor) Start() {
	rpo.stateLock.Lock()
	if rpo.status != common.DaemonStatusInitialized {
		rpo.stateLock.Unlock()
		return
	}
	rpo.status = common.DaemonStatusStarted
	rpo.stateLock.Unlock()

	broadcastAddress, err := rpo.broadcastHostPortResolver()
	if err != nil {
		rpo.logger.Fatal("unable to resolve broadcast address", tag.Error(err))
	}

	// TODO - Note this presents a small race condition as we write our identity before we bootstrap ringpop.
	// This is a current limitation of the current structure of the ringpop library as
	// we must know our seed nodes before bootstrapping

	if err = rpo.startHeartbeat(broadcastAddress); err != nil {
		rpo.logger.Fatal("unable to initialize membership heartbeats", tag.Error(err))
	}

	if err = rpo.bootstrapRingPop(); err != nil {
		// Stop() called during Start()'s execution. This is ok
		if strings.Contains(err.Error(), "destroyed while attempting to join") {
			return
		}
		rpo.logger.Fatal("failed to start ringpop", tag.Error(err))
	}

	labels, err := rpo.rp.Labels()
	if err != nil {
		rpo.logger.Fatal("unable to get ringpop labels", tag.Error(err))
	}

	if until := time.Until(rpo.joinTime); until > 0 && until.Seconds() < maxScheduledEventTimeSeconds {
		if err = labels.Set(startAtKey, strconv.FormatInt(rpo.joinTime.Unix(), 10)); err != nil {
			rpo.logger.Fatal("unable to set ringpop label", tag.Error(err), tag.Key(startAtKey))
		}
		// Clean up the label eventually, but we don't really care when.
		// The jitter doesn't really matter, but if two nodes join/leave at the same aligned
		// time, it just spreads out the membership gossip traffic a little bit.
		clearAfter := until + backoff.Jitter(2*rpo.propagationTime, 0.2)
		time.AfterFunc(clearAfter, rpo.clearStartAt)
	}

	if err = labels.Set(portKey, strconv.Itoa(rpo.services[rpo.serviceName])); err != nil {
		rpo.logger.Fatal("unable to set ringpop label", tag.Error(err), tag.Key(portKey))
	}

	// This label should be set last, it's used as the prediciate for finding members for rings.
	if err = labels.Set(roleKey, string(rpo.serviceName)); err != nil {
		rpo.logger.Fatal("unable to set ringpop label", tag.Error(err), tag.Key(roleKey))
	}

	// Our individual rings may not support concurrent start/stop calls so we reacquire the state lock while acting upon them
	rpo.stateLock.Lock()
	for _, ring := range rpo.rings {
		ring.Start()
	}
	rpo.stateLock.Unlock()

	rpo.initialized.Set(struct{}{}, nil)
}

// bootstrap ring pop service by discovering the bootstrap hosts and joining the ring pop cluster
func (rpo *monitor) bootstrapRingPop() error {
	policy := backoff.NewExponentialRetryPolicy(healthyHostLastHeartbeatCutoff / 2).
		WithBackoffCoefficient(1).
		WithMaximumAttempts(maxBootstrapRetries)
	op := func() error {
		hostPorts, err := rpo.fetchCurrentBootstrapHostports()
		if err != nil {
			return err
		}

		bootParams := &swim.BootstrapOptions{
			ParallelismFactor: 10,
			JoinSize:          1,
			MaxJoinDuration:   rpo.maxJoinDuration,
			DiscoverProvider:  statichosts.New(hostPorts...),
		}

		_, err = rpo.rp.Bootstrap(bootParams)
		if err != nil {
			rpo.logger.Warn("unable to bootstrap ringpop. retrying", tag.Error(err))
		}
		return err
	}

	if err := backoff.ThrottleRetry(op, policy, nil); err != nil {
		return fmt.Errorf("exhausted all retries: %w", err)
	}
	return nil
}

func (rpo *monitor) clearStartAt() {
	// We can ignore all errors here, they're probably because we shut down already.
	// This is just to clean up the startAt label.
	if labels, err := rpo.rp.Labels(); err == nil {
		_, _ = labels.Remove(startAtKey)
	}
}

func (rpo *monitor) WaitUntilInitialized(ctx context.Context) error {
	_, err := rpo.initialized.Get(ctx)
	return err
}

func serviceNameToServiceTypeEnum(name primitives.ServiceName) (persistence.ServiceType, error) {
	switch name {
	case primitives.AllServices:
		return persistence.All, nil
	case primitives.FrontendService:
		return persistence.Frontend, nil
	case primitives.InternalFrontendService:
		return persistence.InternalFrontend, nil
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

func (rpo *monitor) upsertMyMembership(
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

// splitHostPortTyped expands upon net.SplitHostPort by providing type parsing.
func splitHostPortTyped(hostPort string) (net.IP, uint16, error) {
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

func (rpo *monitor) startHeartbeat(broadcastHostport string) error {
	// Start by cleaning up expired records to avoid growth
	err := rpo.metadataManager.PruneClusterMembership(rpo.lifecycleCtx, &persistence.PruneClusterMembershipRequest{MaxRecordsPruned: 10})
	if err != nil {
		return err
	}

	sessionStarted := time.Now().UTC()

	// Parse and validate broadcast hostport
	broadcastAddress, broadcastPort, err := splitHostPortTyped(broadcastHostport)
	if err != nil {
		return err
	}

	// Parse and validate existing service name
	role, err := serviceNameToServiceTypeEnum(rpo.serviceName)
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
	err = rpo.upsertMyMembership(rpo.lifecycleCtx, req)
	if err == nil {
		rpo.logger.Info("Membership heartbeat upserted successfully",
			tag.Address(broadcastAddress.String()),
			tag.Port(int(broadcastPort)),
			tag.HostID(rpo.hostID.String()))

		rpo.startHeartbeatUpsertLoop(req)
	}

	return err
}

func (rpo *monitor) fetchCurrentBootstrapHostports() ([]string, error) {
	pageSize := 1000
	set := make(map[string]struct{})

	var nextPageToken []byte

	for {
		resp, err := rpo.metadataManager.GetClusterMembers(
			rpo.lifecycleCtx,
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

			rpo.logger.Info("bootstrap hosts fetched", tag.BootstrapHostPorts(strings.Join(bootstrapHostPorts, ",")))
			return bootstrapHostPorts, nil
		}
	}
}

func (rpo *monitor) startHeartbeatUpsertLoop(request *persistence.UpsertClusterMembershipRequest) {
	loopUpsertMembership := func() {
		for {
			select {
			case <-rpo.lifecycleCtx.Done():
				return
			default:
			}
			err := rpo.upsertMyMembership(rpo.lifecycleCtx, request)

			if err != nil {
				rpo.logger.Error("Membership upsert failed.", tag.Error(err))
			}

			jitter := math.Round(rand.Float64() * 5)
			time.Sleep(time.Second * time.Duration(10+jitter))
		}
	}

	go loopUpsertMembership()
}

// Stop the membership monitor and all associated rings. This holds the state lock
// for the entire call as the individual ring Start/Stop functions may not be safe to
// call concurrently
func (rpo *monitor) Stop() {
	rpo.stateLock.Lock()
	defer rpo.stateLock.Unlock()
	if rpo.status != common.DaemonStatusStarted {
		return
	}
	rpo.status = common.DaemonStatusStopped

	rpo.lifecycleCancel()

	for _, ring := range rpo.rings {
		ring.Stop()
	}

	rpo.rp.Destroy()
}

func (rpo *monitor) EvictSelf() error {
	return rpo.rp.SelfEvict()
}

func (rpo *monitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	until := time.Until(asOf)
	if until <= 0 || until.Seconds() >= maxScheduledEventTimeSeconds {
		return 0, rpo.rp.SelfEvict()
	}
	// set label for eviction time in the future
	labels, err := rpo.rp.Labels()
	if err != nil {
		rpo.logger.Error("unable to set ringpop label", tag.Error(err), tag.Key(stopAtKey))
		return 0, err
	}
	err = labels.Set(stopAtKey, strconv.FormatInt(asOf.Unix(), 10))
	if err != nil {
		rpo.logger.Error("unable to set ringpop label", tag.Error(err), tag.Key(stopAtKey))
		return 0, err
	}
	// Wait a couple more seconds after the stopAt time before actually leaving.
	// The jitter doesn't really matter, but if two nodes join/leave at the same aligned time,
	// it just spreads out the membership gossip traffic a little bit.
	leaveAfter := until + backoff.Jitter(rpo.propagationTime, 0.2)
	time.AfterFunc(leaveAfter, func() { _ = rpo.rp.SelfEvict() })
	return leaveAfter + rpo.propagationTime, nil
}

func (rpo *monitor) GetResolver(service primitives.ServiceName) (membership.ServiceResolver, error) {
	ring, found := rpo.rings[service]
	if !found {
		return nil, membership.ErrUnknownService
	}
	return ring, nil
}

func (rpo *monitor) GetReachableMembers() ([]string, error) {
	return rpo.rp.GetReachableMembers()
}

func (rpo *monitor) SetDraining(draining bool) error {
	labels, err := rpo.rp.Labels()
	if err != nil {
		// This only happens if ringpop is not bootstrapped yet.
		return err
	}
	return labels.Set(drainingKey, strconv.FormatBool(draining))
}

func (rpo *monitor) ApproximateMaxPropagationTime() time.Duration {
	return rpo.propagationTime
}

func replaceServicePort(address string, servicePort int) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", membership.ErrIncorrectAddressFormat
	}
	return net.JoinHostPort(host, convert.IntToString(servicePort)), nil
}
