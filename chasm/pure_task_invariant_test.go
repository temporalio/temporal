package chasm

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// invariantTree builds a single-component tree holding one expired pure task
// whose physical timer has already been created and fired.
func (s *nodeSuite) invariantTree(now time.Time) *Node {
	taskBlob, err := encodeChasmBlob(&commonpb.Payload{Data: []byte("task")})
	s.NoError(err)

	root, err := s.newTestTree(map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data:                      taskBlob,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	s.NotNil(root)
	return root
}

// runInvariantSweep fires the tree's due pure tasks through ExecutePureTask.
// validAfterExecution selects whether the component's validator keeps accepting
// its task once it has run - i.e. whether it violates the invariant.
func (s *nodeSuite) runInvariantSweep(validAfterExecution bool) error {
	s.timeSource.Update(time.Now())
	now := s.timeSource.Now()
	root := s.invariantTree(now)

	validateCalls := 0
	s.testLibrary.mockPureTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ Context, _ any, _ TaskInvocation, _ *TestPureTask) (bool, error) {
			validateCalls++
			if validateCalls == 1 {
				return true, nil // pre-execution gate
			}
			return validAfterExecution, nil
		}).AnyTimes()

	s.testLibrary.mockPureTaskHandler.EXPECT().
		Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		Times(1)

	return root.EachPureTask(now.Add(time.Minute),
		func(handler NodePureTask, attrs TaskAttributes, taskInstance any) (bool, error) {
			return handler.ExecutePureTask(context.Background(), attrs, taskInstance)
		})
}

// A component that invalidates its own task, which is what every component must
// do, sweeps cleanly.
func (s *nodeSuite) TestEachPureTask_InvalidatedTaskIsAccepted() {
	s.NoError(s.runInvariantSweep(false))
}

// A component whose task is still valid after executing gets an internal error.
// Without this, the task is never pruned, stays past-due as the tree's earliest
// pure task, and - already carrying physicalTaskStatusCreated - suppresses
// physical timer generation for every pure task in the execution.
func (s *nodeSuite) TestEachPureTask_StillValidTaskIsRejected() {
	// The assertion logs at Error level via softassert; tell the test logger that
	// this specific failed assertion is the expected outcome.
	s.logger.(*testlogger.TestLogger).Expect(testlogger.Error, ".*", tag.FailedAssertion)

	err := s.runInvariantSweep(true)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.Contains(err.Error(), "pure task is still valid after being executed")
}

// The assertion must not fire for a component that completes by deleting itself:
// its node leaves the tree, so no task is left behind to strand a timer. Such a
// component never gets the chance to invalidate its own task, so asserting here
// would reject correct behaviour.
func (s *nodeSuite) TestEachPureTask_SelfDeletingComponentIsExempt() {
	s.timeSource.Update(time.Now())
	now := s.timeSource.Now()

	taskBlob, err := encodeChasmBlob(&commonpb.Payload{Data: []byte("task")})
	s.NoError(err)
	root, err := s.newTestTree(map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
			},
		},
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent1TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data:                      taskBlob,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	// Always valid: this component would violate the invariant if it survived.
	s.testLibrary.mockPureTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(true, nil).
		AnyTimes()

	// Executing drops the subcomponent holding the task, taking its node out of
	// the tree.
	s.testLibrary.mockPureTaskHandler.EXPECT().
		Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx MutableContext, _ any, _ TaskAttributes, _ *TestPureTask) error {
			rootComponent, err := root.Component(ctx, ComponentRef{})
			if err != nil {
				return err
			}
			rootComponent.(*TestComponent).SubComponent1 = NewEmptyField[*TestSubComponent1]()
			return nil
		}).Times(1)

	s.NoError(root.EachPureTask(now.Add(time.Minute),
		func(handler NodePureTask, attrs TaskAttributes, taskInstance any) (bool, error) {
			return handler.ExecutePureTask(context.Background(), attrs, taskInstance)
		}))
}
