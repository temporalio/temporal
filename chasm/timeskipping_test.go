package chasm

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// timeSkippingTaskNodes builds a tree with three CategoryTimer tasks already materialized
// (PhysicalTaskStatus == Created): a side-effect timer on the root, a side-effect timer on a child,
// and a pure timer on the root. The scheduled times are in the (virtual) future and carry no
// destination, so taskCategory classifies the side-effect tasks as CategoryTimer.
func (s *nodeSuite) timeSkippingTaskNodes() map[string]*persistencespb.ChasmNode {
	now := s.timeSource.Now()
	return map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Hour)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Hour)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent1TypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(2 * time.Hour)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
	}
}

// TestRegenerateTasksForTimeSkipping_RestampsEachTimerExactlyOnce verifies a time-skipping
// regeneration re-emits exactly one physical task per logical CategoryTimer task — two side-effect
// timers plus a single pure timer (the earliest across the tree) — and not duplicates. This is the
// "generated only once" guarantee: even though every task starts already Created, the re-stamp emits
// each one exactly once.
func (s *nodeSuite) TestRegenerateTasksForTimeSkipping_RestampsEachTimerExactlyOnce() {
	root, err := s.newTestTree(s.timeSkippingTaskNodes())
	s.NoError(err)

	// regenerateTasksForTimeSkipping validates each task before re-stamping it; all three test timers
	// are valid.
	s.testLibrary.mockSideEffectTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(true, nil).AnyTimes()
	s.testLibrary.mockPureTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(true, nil).AnyTimes()

	err = root.regenerateChasmTasksForTimeSkipping(NewContext(context.Background(), root))
	s.NoError(err)

	// 2 side-effect CategoryTimer tasks (root + SubComponent1) + 1 pure timer (earliest) = 3, all
	// CategoryTimer. Not 6 (would indicate double-emission) and not 2 (would indicate the no-break
	// re-stamp skipped already-Created tasks).
	s.Equal(3, s.nodeBackend.NumTasksAdded())
	s.Len(s.nodeBackend.TasksByCategory[tasks.CategoryTimer], 3)
}

// TestCloseTransaction_DoesNotRegenerateAlreadyMaterializedTimers verifies the complementary
// property: a plain CloseTransaction (no time-skipping transition, no user-state change) does NOT
// re-emit timer tasks that are already materialized. This is what keeps the active cluster from
// generating new timer tasks on every transaction — only a skip (via regenerateTasksForTimeSkipping)
// re-stamps them.
func (s *nodeSuite) TestCloseTransaction_DoesNotRegenerateAlreadyMaterializedTimers() {
	root, err := s.newTestTree(s.timeSkippingTaskNodes())
	s.NoError(err)

	// Time skipping is not enabled (the mock backend's GetExecutionInfo returns nil, so TimeSkippingInfo
	// is nil) and nothing was mutated, so the normal generation loop must skip the already-Created timer
	// tasks.
	_, err = root.CloseTransaction()
	s.NoError(err)
	s.Equal(0, s.nodeBackend.NumTasksAdded())
}
