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
		logger                 bark.Logger
	}
)

func newMutableStateBuilder(logger bark.Logger) *mutableStateBuilder {
	return &mutableStateBuilder{
		updateActivityInfos:    []*persistence.ActivityInfo{},
		pendingActivityInfoIDs: make(map[int64]*persistence.ActivityInfo),
		logger:                 logger}
}

func (e *mutableStateBuilder) Load(activityInfos map[int64]*persistence.ActivityInfo) {
	e.pendingActivityInfoIDs = activityInfos
}

func (e *mutableStateBuilder) isActivityHeartBeatRunning(scheduleEventID int64) (bool, *persistence.ActivityInfo) {
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
