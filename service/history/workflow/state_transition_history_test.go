package workflow_test

import (
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/workflow"
)

func TestUpdatedTranstionHistory(t *testing.T) {
	var hist []*persistencespb.VersionedTransition
	hist = workflow.UpdatedTransitionHistory(hist, 1)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 1}.Build()},
		hist,
	)
	hist = workflow.UpdatedTransitionHistory(hist, 1)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 2}.Build()},
		hist,
	)
	hist = workflow.UpdatedTransitionHistory(hist, 2)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 2}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 3}.Build(),
		},
		hist,
	)
	hist = workflow.UpdatedTransitionHistory(hist, 2)
	protorequire.ProtoSliceEqual(t,
		[]*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 2}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 4}.Build(),
		},
		hist,
	)
}
