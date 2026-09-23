package replication

import (
	"fmt"
	"strings"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/queues"
)

const namespaceLaneKeyPrefix = "namespace:"

type replicationLaneDirectiveKind int

const (
	replicationLaneCreate replicationLaneDirectiveKind = iota + 1
	replicationLaneReclassify
	replicationLaneRelease
	replicationLaneKeep
)

type replicationLaneDirective struct {
	kind       replicationLaneDirectiveKind
	logicalKey string
	class      replicationLaneClass
	scope      queues.Scope
}

type replicationLanePolicySignals struct {
	throttleHighNamespaceIDs []string
	sharedHighWatermark      int64
}

type replicationLanePolicy interface {
	Evaluate(signals replicationLanePolicySignals, lanes []senderLaneSnapshot) []replicationLaneDirective
	ClassCount() int
	LaneCreated(logicalKey string)
	LaneRetired(logicalKey string)
}

type namespaceIsolationPolicy struct {
	classCount     int
	demotionCycles int
	cooldownCycles int
	streaks        map[string]*namespaceLaneStreak
}

// namespaceIsolationPolicy interprets namespace signals, but never assigns wire
// IDs, persists state, or sends messages; those concerns remain in the registry.

type namespaceLaneStreak struct {
	throttled int
	calm      int
}

func newNamespaceIsolationPolicy(classCount, demotionCycles, cooldownCycles int) *namespaceIsolationPolicy {
	return &namespaceIsolationPolicy{
		classCount:     max(1, classCount),
		demotionCycles: max(1, demotionCycles),
		cooldownCycles: max(1, cooldownCycles),
		streaks:        make(map[string]*namespaceLaneStreak),
	}
}

func (p *namespaceIsolationPolicy) ClassCount() int {
	return p.classCount
}

func (p *namespaceIsolationPolicy) LaneRetired(logicalKey string) {
	delete(p.streaks, logicalKey)
}

func (p *namespaceIsolationPolicy) LaneCreated(logicalKey string) {
	p.streaks[logicalKey] = &namespaceLaneStreak{throttled: 1}
}

func (p *namespaceIsolationPolicy) Evaluate(
	signals replicationLanePolicySignals,
	lanes []senderLaneSnapshot,
) []replicationLaneDirective {
	throttledNamespaceIDs := signals.throttleHighNamespaceIDs
	throttled := make(map[string]struct{}, len(throttledNamespaceIDs))
	for _, namespaceID := range throttledNamespaceIDs {
		throttled[namespaceID] = struct{}{}
	}
	laneByKey := make(map[string]senderLaneSnapshot, len(lanes))
	for _, lane := range lanes {
		laneByKey[lane.logicalKey] = lane
	}

	var directives []replicationLaneDirective
	for _, lane := range lanes {
		if !strings.HasPrefix(lane.logicalKey, namespaceLaneKeyPrefix) {
			continue
		}
		namespaceID := strings.TrimPrefix(lane.logicalKey, namespaceLaneKeyPrefix)
		streak := p.streaks[lane.logicalKey]
		if streak == nil {
			streak = &namespaceLaneStreak{}
			p.streaks[lane.logicalKey] = streak
		}
		if _, ok := throttled[namespaceID]; ok {
			streak.calm = 0
			streak.throttled++
			directives = append(directives, replicationLaneDirective{kind: replicationLaneKeep, logicalKey: lane.logicalKey})
			if streak.throttled >= p.demotionCycles && int(lane.class) < p.classCount {
				streak.throttled = 0
				directives = append(directives, replicationLaneDirective{
					kind:       replicationLaneReclassify,
					logicalKey: lane.logicalKey,
					class:      lane.class + 1,
				})
			}
			continue
		}
		streak.throttled = 0
		streak.calm++
		if streak.calm >= p.cooldownCycles {
			directives = append(directives, replicationLaneDirective{kind: replicationLaneRelease, logicalKey: lane.logicalKey})
		}
	}

	for _, namespaceID := range throttledNamespaceIDs {
		logicalKey := namespaceLaneKeyPrefix + namespaceID
		if _, ok := laneByKey[logicalKey]; ok {
			continue
		}
		directives = append(directives, replicationLaneDirective{
			kind:       replicationLaneCreate,
			logicalKey: logicalKey,
			class:      1,
			scope:      namespaceLaneScope(namespaceID, signals.sharedHighWatermark),
		})
	}
	return directives
}

type senderLaneController struct {
	registry *senderLaneRegistry
	policy   replicationLanePolicy
	maxLanes int
	observer *replicationLaneTransitionObserver
}

// senderLaneController is the boundary between policy directives and lane
// lifecycle. This is where admission limits and transition observation apply.

func newSenderLaneController(
	registry *senderLaneRegistry,
	policy replicationLanePolicy,
	maxLanes int,
	logger log.Logger,
) *senderLaneController {
	return &senderLaneController{
		registry: registry,
		policy:   policy,
		maxLanes: maxLanes,
		observer: &replicationLaneTransitionObserver{
			logger: logger,
			// Denials fire once per throttled namespace per reconcile cycle, so they
			// are throttled separately from the rare transition logs.
			throttledLogger: log.NewThrottledLogger(logger, func() float64 { return 1 }),
		},
	}
}

func (c *senderLaneController) Reconcile(
	signals replicationLanePolicySignals,
	laneStates map[string]*replicationspb.ReplicationState,
) ([]replicationLaneClass, error) {
	c.registry.ObserveAcks(laneStates)
	lanes := c.registry.Snapshots()
	laneCount := len(lanes)
	for _, directive := range c.policy.Evaluate(signals, lanes) {
		switch directive.kind {
		case replicationLaneCreate:
			if c.maxLanes > 0 && laneCount >= c.maxLanes {
				c.observer.CreationDenied(directive.logicalKey, laneCount, c.maxLanes)
				continue
			}
			lane, created, err := c.registry.Create(
				directive.logicalKey,
				directive.scope,
				directive.class,
			)
			if err != nil {
				return nil, err
			}
			if created {
				laneCount++
				c.policy.LaneCreated(lane.logicalKey)
				c.observer.Created(lane)
			}
		case replicationLaneReclassify:
			if lane, changed := c.registry.SetClass(directive.logicalKey, directive.class); changed {
				c.observer.Reclassified(lane)
			}
		case replicationLaneRelease:
			if c.registry.RequestRetirement(directive.logicalKey) {
				if lane, ok := c.registry.SnapshotByKey(directive.logicalKey); ok {
					c.observer.RetirementRequested(lane)
				}
			}
		case replicationLaneKeep:
			c.registry.CancelRetirement(directive.logicalKey)
		default:
			return nil, fmt.Errorf("unknown replication lane directive kind: %d", directive.kind)
		}
	}
	return runnableLaneClasses(lanes, c.registry.Snapshots()), nil
}

func runnableLaneClasses(before, after []senderLaneSnapshot) []replicationLaneClass {
	previous := make(map[string]senderLaneSnapshot, len(before))
	for _, lane := range before {
		previous[lane.logicalKey] = lane
	}
	var classes []replicationLaneClass
	for _, lane := range after {
		prior, existed := previous[lane.logicalKey]
		if !existed || prior.class != lane.class || (prior.retiring && !lane.retiring) {
			classes = append(classes, lane.class)
		}
	}
	return classes
}

func (c *senderLaneController) CompleteRetirement(laneID string) bool {
	lane, ok := c.registry.CompleteRetirement(laneID)
	if ok {
		c.policy.LaneRetired(lane.logicalKey)
		c.observer.Retired(lane)
	}
	return ok
}

type replicationLaneTransitionObserver struct {
	logger          log.Logger
	throttledLogger log.Logger
}

func (o *replicationLaneTransitionObserver) CreationDenied(logicalKey string, laneCount, maxLanes int) {
	o.throttledLogger.Warn("Replication lane creation denied: lane limit reached",
		tag.NewStringTag("replication-lane-logical-key", logicalKey),
		tag.NewInt("replication-lane-count", laneCount),
		tag.NewInt("replication-lane-max", maxLanes),
	)
}

func (o *replicationLaneTransitionObserver) Created(lane senderLaneSnapshot) {
	o.logger.Info("Replication lane created", replicationLaneTags(lane)...)
}

func (o *replicationLaneTransitionObserver) Reclassified(lane senderLaneSnapshot) {
	o.logger.Info("Replication lane reclassified", replicationLaneTags(lane)...)
}

func (o *replicationLaneTransitionObserver) RetirementRequested(lane senderLaneSnapshot) {
	o.logger.Info("Replication lane retirement requested", replicationLaneTags(lane)...)
}

func (o *replicationLaneTransitionObserver) Retired(lane senderLaneSnapshot) {
	o.logger.Info("Replication lane retired", replicationLaneTags(lane)...)
}

func replicationLaneTags(lane senderLaneSnapshot) []tag.Tag {
	return []tag.Tag{
		tag.NewStringTag("replication-lane-id", lane.id),
		tag.NewStringTag("replication-lane-logical-key", lane.logicalKey),
		tag.NewInt("replication-lane-class", int(lane.class)),
	}
}
