package concurrency

import (
	"slices"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TODO(fc): move to dynamic config
const reserveTimeout = 30 * time.Second
const stagedWakeInterval = time.Second

const initialLimit = int32(1_000_000)

type Component struct {
	chasm.UnimplementedComponent

	*fcpb.ConcurrencyState
}

func (c *Component) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	// TODO(fc): we should be able to clean these up if they've been idle a while
	return chasm.LifecycleStateRunning
}

func (c *Component) Terminate(_ chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	// TODO(fc): can we block this if there are any committed slots?
	return chasm.TerminateComponentResponse{}, nil
}

func (c *Component) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

func expiredReservation(slot *fcpb.ConcurrencyState_Slot, nowSec int64) bool {
	return !slot.Committed && slot.Expires != nil && slot.Expires.Seconds < nowSec+1
}

func (c *Component) expire(now time.Time) {
	nowSec := now.Unix()
	c.Slots = slices.DeleteFunc(c.Slots, func(slot *fcpb.ConcurrencyState_Slot) bool {
		return expiredReservation(slot, nowSec)
	})
}

func (c *Component) find(slotID string) int {
	return slices.IndexFunc(c.Slots, func(slot *fcpb.ConcurrencyState_Slot) bool {
		return slot.SlotId == slotID
	})
}

func (c *Component) availableSlots() int32 {
	return max(0, c.Config.ConcurrentTasks-int32(len(c.Slots)))
}

// effectiveAvailableSlots is called from PollComponent and should not modify the state.
func (c *Component) effectiveAvailableSlots(now time.Time) int32 {
	// like c.expire(now) + c.availableSlots(), but without modifying state
	nowSec := now.Unix()
	usedSlots := int32(0)
	for _, slot := range c.Slots {
		if !expiredReservation(slot, nowSec) {
			usedSlots++
		}
	}
	return max(0, c.Config.ConcurrentTasks-usedSlots)
}

func (c *Component) incrementGeneration() {
	// this causes all polls to block until WakeUpTo or WakeAll advances
	c.Generation++
	c.WakeUpTo = 0
	c.WakeAll = false
	c.WakeStage = 0
}

func (c *Component) updateConfig(config *taskqueuepb.ConcurrencyLimit, version int64) {
	if config != nil && version > c.ConfigVersion {
		c.Config = config
		c.ConfigVersion = version
	}
}

func (c *Component) reserve(slotID string, now time.Time) bool {
	if c.find(slotID) >= 0 {
		return true // already reserved or committed, accept
	}
	if c.availableSlots() <= 0 {
		return false
	}
	c.Slots = append(c.Slots, &fcpb.ConcurrencyState_Slot{
		SlotId:    slotID,
		Committed: false,
		// Truncate is okay because expire adds a second anyway.
		Expires: timestamppb.New(now.Add(reserveTimeout).Truncate(time.Second)),
	})
	return true
}

func (c *Component) cancelReservation(slotID string) {
	c.Slots = slices.DeleteFunc(c.Slots, func(slot *fcpb.ConcurrencyState_Slot) bool {
		return !slot.Committed && slot.SlotId == slotID
	})
}

func (c *Component) commit(slotID string) bool {
	if idx := c.find(slotID); idx >= 0 {
		// note this works even if it was already committed
		c.Slots[idx].Committed = true
		c.Slots[idx].Expires = nil
		return true
	}

	// TODO(fc): consider fast-recover if free slots
	return false
}

func (c *Component) release(slotID string) {
	c.Slots = slices.DeleteFunc(c.Slots, func(slot *fcpb.ConcurrencyState_Slot) bool {
		return slot.Committed && slot.SlotId == slotID
	})
}

// poll is called from PollComponent and should not modify the state.
func (c *Component) poll(now time.Time, reqGeneration int64, reqStartTime int64, reqTokens int32) (int64, int32, bool) {
	if reqGeneration < c.Generation {
		return c.Generation, 0, true // old generation, try again with current
	} else if reqGeneration > c.Generation {
		return 0, 0, false // shouldn't happen, but wait to keep things monotonic
	}

	wake := c.WakeAll || reqStartTime <= c.WakeUpTo
	storedAvailable := c.availableSlots()
	effectiveAvailable := min(c.effectiveAvailableSlots(now), reqTokens)
	freedByExpiration := storedAvailable == 0 && effectiveAvailable > 0

	if wake || freedByExpiration {
		// If this waiter is woken, we can return. Also if slots were freed by expiration: we
		// won't get a state transition to set WakeUpTo/WakeAll, so we just return here.
		return c.Generation, effectiveAvailable, true
	}

	return 0, 0, false // wait for another transition
}
