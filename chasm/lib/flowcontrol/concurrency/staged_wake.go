package concurrency

import "go.temporal.io/server/chasm"

const maxStagedWakeStage = 10

type StagedWakeHandler struct {
	chasm.PureTaskHandlerBase

	handler *Handler
}

func NewStagedWakeHandler(handler *Handler) *StagedWakeHandler {
	return &StagedWakeHandler{
		handler: handler,
	}
}

type stagedWake struct{}

func (t *StagedWakeHandler) Validate(ctx chasm.Context, c *Component, _ chasm.TaskInvocation, _ *stagedWake) (bool, error) {
	// If we have no slots left, there's nothing to do. And if WakeAll is already set, there's nothing to do.
	ok := c.availableSlots() > 0 && !c.WakeAll
	return ok, nil
}

func (t *StagedWakeHandler) Execute(cctx chasm.MutableContext, c *Component, _ chasm.TaskAttributes, _ *stagedWake) error {
	getWakeLevel := func(wantTokens int32) (int64, bool) {
		key := batchKey{
			namespaceID: cctx.ExecutionKey().NamespaceID,
			key:         cctx.ExecutionKey().BusinessID,
		}
		return t.handler.getWakeLevel(key, wantTokens)
	}

	c.expire(cctx.Now(c))

	// double number woken at each stage
	c.WakeStage++
	doWake(cctx, c, getWakeLevel)

	return nil
}

func doWake(cctx chasm.MutableContext, c *Component, getWakeTime func(int32) (int64, bool)) {
	if c.WakeStage >= maxStagedWakeStage {
		c.WakeUpTo, c.WakeAll = 0, true
		return
	}

	wantTokens := c.availableSlots() << c.WakeStage
	if wantTokens <= 0 || c.WakeAll {
		return // no slots available or done, return without modifying wake state
	}

	wakeUpTo, wakeAll := getWakeTime(wantTokens)
	c.WakeUpTo = max(c.WakeUpTo, wakeUpTo)
	c.WakeAll = wakeAll

	if !c.WakeAll { // not all woken yet, add task for more
		cctx.AddTask(
			c,
			chasm.TaskAttributes{ScheduledTime: cctx.Now(c).Add(stagedWakeInterval)},
			&stagedWake{},
		)
	}
}
