package history

import (
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"
	"github.com/uber-common/bark"
)

type (
	mutableStateBuilder struct {
		pendingActivityInfoIDs map[int64]*persistence.ActivityInfo
		updateActivityInfos    []*persistence.ActivityInfo
		deleteActivityInfo     *int64
		pendingTimerInfoIDs    map[string]*persistence.TimerInfo
		updateTimerInfos       []*persistence.TimerInfo
		deleteTimerInfos       []string
		logger                 bark.Logger
	}
)

func newMutableStateBuilder(logger bark.Logger) *mutableStateBuilder {
	return &mutableStateBuilder{
		updateActivityInfos:    []*persistence.ActivityInfo{},
		pendingActivityInfoIDs: make(map[int64]*persistence.ActivityInfo),
		pendingTimerInfoIDs:    make(map[string]*persistence.TimerInfo),
		updateTimerInfos:       []*persistence.TimerInfo{},
		deleteTimerInfos:       []string{},
		logger:                 logger}
}

func (e *mutableStateBuilder) Load(activityInfos map[int64]*persistence.ActivityInfo,
	timerInfos map[string]*persistence.TimerInfo) {
	e.pendingActivityInfoIDs = activityInfos
	e.pendingTimerInfoIDs = timerInfos
}

func (e *mutableStateBuilder) isActivityRunning(scheduleEventID int64) (bool, *persistence.ActivityInfo) {
	a, ok := e.pendingActivityInfoIDs[scheduleEventID]
	return ok, a
}

func (e *mutableStateBuilder) UpdatePendingActivity(scheduleEventID int64, ai *persistence.ActivityInfo) {
	e.pendingActivityInfoIDs[scheduleEventID] = ai
	e.updateActivityInfos = append(e.updateActivityInfos, ai)
}

func (e *mutableStateBuilder) DeletePendingActivity(scheduleEventID int64) {
	e.deleteActivityInfo = common.Int64Ptr(scheduleEventID)
}

func (e *mutableStateBuilder) isTimerRunning(timerID string) (bool, *persistence.TimerInfo) {
	a, ok := e.pendingTimerInfoIDs[timerID]
	return ok, a
}

func (e *mutableStateBuilder) UpdatePendingTimers(timerID string, ti *persistence.TimerInfo) {
	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos = append(e.updateTimerInfos, ti)
}

func (e *mutableStateBuilder) DeletePendingTimer(timerID string) {
	e.deleteTimerInfos = append(e.deleteTimerInfos, timerID)
}
