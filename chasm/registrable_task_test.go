package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

func TestResolveTaskCountMetricThreshold(t *testing.T) {
	testCases := []struct {
		name                   string
		task                   *RegistrableTask
		dynamicConfigThreshold int
		expected               int
	}{
		{
			name:     "framework default",
			task:     newTestRegistrableTask(WithTaskCountMetric(0)),
			expected: defaultTaskCountMetricThreshold,
		},
		{
			name:     "registered threshold",
			task:     newTestRegistrableTask(WithTaskCountMetric(50)),
			expected: 50,
		},
		{
			name:     "non-positive registered threshold falls back to default",
			task:     newTestRegistrableTask(WithTaskCountMetric(-1)),
			expected: defaultTaskCountMetricThreshold,
		},
		{
			name:                   "dynamic config overrides default",
			task:                   newTestRegistrableTask(WithTaskCountMetric(0)),
			dynamicConfigThreshold: 20,
			expected:               20,
		},
		{
			name:                   "dynamic config overrides registered threshold",
			task:                   newTestRegistrableTask(WithTaskCountMetric(50)),
			dynamicConfigThreshold: 20,
			expected:               20,
		},
		{
			name:                   "negative dynamic config disables",
			task:                   newTestRegistrableTask(WithTaskCountMetric(50)),
			dynamicConfigThreshold: -1,
			expected:               -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.task.resolveTaskCountMetricThreshold(tc.dynamicConfigThreshold))
		})
	}
}

func TestRegistryTaskCountMetricComponentIDs(t *testing.T) {
	testCases := []struct {
		name        string
		task        func(ctrl *gomock.Controller) *RegistrableTask
		expected    []string
		expectedErr string
	}{
		{
			name: "concrete component type",
			task: func(ctrl *gomock.Controller) *RegistrableTask {
				return NewRegistrableSideEffectTask(
					"task",
					NewMockSideEffectTaskHandler[*TestSubComponent1, *TestSideEffectTask](ctrl),
					WithTaskCountMetric(0),
				)
			},
			expected: []string{"sub1"},
		},
		{
			name: "interface component type",
			task: func(ctrl *gomock.Controller) *RegistrableTask {
				return NewRegistrableSideEffectTask(
					"task",
					NewMockSideEffectTaskHandler[any, *TestSideEffectTask](ctrl),
					WithTaskCountMetric(0),
				)
			},
			expected: []string{"sub1", "sub11"},
		},
		{
			name: "not opted in",
			task: func(ctrl *gomock.Controller) *RegistrableTask {
				return NewRegistrableSideEffectTask(
					"task",
					NewMockSideEffectTaskHandler[*TestSubComponent1, *TestSideEffectTask](ctrl),
				)
			},
			expected: nil,
		},
		{
			name: "component not registered",
			task: func(ctrl *gomock.Controller) *RegistrableTask {
				return NewRegistrableSideEffectTask(
					"task",
					NewMockSideEffectTaskHandler[*TestSubComponent2, *TestSideEffectTask](ctrl),
					WithTaskCountMetric(0),
				)
			},
			expectedErr: "is not registered",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			lib := NewMockLibrary(ctrl)
			lib.EXPECT().Name().Return("lib").AnyTimes()
			lib.EXPECT().Components().Return([]*RegistrableComponent{
				NewRegistrableComponent[*TestSubComponent1]("sub1"),
				NewRegistrableComponent[*TestSubComponent11]("sub11"),
			})
			lib.EXPECT().Tasks().Return([]*RegistrableTask{tc.task(ctrl)})
			lib.EXPECT().NexusServices().Return(nil).AnyTimes()
			lib.EXPECT().NexusServiceProcessors().Return(nil).AnyTimes()

			registry := NewRegistry(log.NewNoopLogger())
			err := registry.Register(lib)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			var actual []string
			for id := range registry.taskCountMetricComponentIDs {
				rc, ok := registry.ComponentByID(id)
				require.True(t, ok)
				actual = append(actual, rc.componentType)
			}
			require.ElementsMatch(t, tc.expected, actual)
		})
	}
}

func newTestRegistrableTask(opts ...RegistrableTaskOption) *RegistrableTask {
	rt := &RegistrableTask{}
	for _, opt := range opts {
		opt(rt)
	}
	return rt
}
