package shard

import (
	"context"
	"time"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	// ownership acts as intermediary between membership and the shard controller.
	// Upon receiving membership update events, it calls the controller's
	// acquireShards method, which acquires or closes shards as needed.
	// The controller calls its verifyOwnership method when asked to
	// acquire a shard, to check that membership believes this host should
	// own the shard.
	ownership struct {
		acquireCh              chan struct{}
		config                 *configs.Config
		goros                  goro.Group
		historyServiceResolver membership.ServiceResolver
		hostInfoProvider       membership.HostInfoProvider
		logger                 log.Logger
		membershipUpdateCh     chan *membership.ChangedEvent
		metricsHandler         metrics.Handler
	}
)

func newOwnership(
	config *configs.Config,
	historyServiceResolver membership.ServiceResolver,
	hostInfoProvider membership.HostInfoProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ownership {
	hostIdentity := hostInfoProvider.HostInfo().Identity()
	logger = log.With(logger, tag.ComponentShardController, tag.Address(hostIdentity))
	return &ownership{
		acquireCh:              make(chan struct{}, 1),
		config:                 config,
		historyServiceResolver: historyServiceResolver,
		hostInfoProvider:       hostInfoProvider,
		logger:                 logger,
		membershipUpdateCh:     make(chan *membership.ChangedEvent, 1),
		metricsHandler:         metricsHandler,
	}
}

func (o *ownership) start(controller *ControllerImpl) {
	o.goros.Go(func(ctx context.Context) error {
		o.eventLoop(ctx)
		return nil
	})

	o.goros.Go(func(ctx context.Context) error {
		o.acquireLoop(ctx, controller)
		return nil
	})

	if err := o.historyServiceResolver.AddListener(
		shardControllerMembershipUpdateListenerName,
		o.membershipUpdateCh,
	); err != nil {
		o.logger.Fatal("Error adding listener", tag.Error(err))
	}
}

func (o *ownership) eventLoop(ctx context.Context) {
	acquireTicker := time.NewTicker(o.config.AcquireShardInterval())
	defer acquireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-acquireTicker.C:
			o.scheduleAcquire()
		case changedEvent := <-o.membershipUpdateCh:
			metrics.MembershipChangedCounter.With(o.metricsHandler).Record(1)

			o.logger.Info("", tag.ValueRingMembershipChangedEvent,
				tag.NumberProcessed(len(changedEvent.HostsAdded)),
				tag.NumberDeleted(len(changedEvent.HostsRemoved)),
				tag.NumberChanged(len(changedEvent.HostsChanged)),
			)

			o.scheduleAcquire()
		}
	}
}

func (o *ownership) scheduleAcquire() {
	select {
	case o.acquireCh <- struct{}{}:
	default:
	}
}

func (o *ownership) acquireLoop(ctx context.Context, controller *ControllerImpl) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.acquireCh:
			controller.acquireShards(ctx)
		}
	}
}

func (o *ownership) stop() {
	if err := o.historyServiceResolver.RemoveListener(
		shardControllerMembershipUpdateListenerName,
	); err != nil {
		o.logger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
	}

	o.goros.Cancel()
	o.goros.Wait()
}

// verifyOwnership checks if the shard should be owned by this host's shard
// controller. If membership lists another host as the owner, it returns a
// ShardOwnershipLost error with the correct owner.
func (o *ownership) verifyOwnership(shardID int32) error {
	ownerInfo, err := o.historyServiceResolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return err
	}

	hostInfo := o.hostInfoProvider.HostInfo()
	if ownerInfo.Identity() != hostInfo.Identity() {
		return serviceerrors.NewShardOwnershipLost(ownerInfo.Identity(), hostInfo.GetAddress())
	}

	return nil
}
