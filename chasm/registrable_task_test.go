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
	type testLibrary struct {
		components []*RegistrableComponent
		tasks      []*RegistrableTask
	}
	components := func() []*RegistrableComponent {
		return []*RegistrableComponent{
			NewRegistrableComponent[*TestSubComponent1]("sub1"),
			NewRegistrableComponent[*TestSubComponent11]("sub11"),
		}
	}

	testCases := []struct {
		name string
		// Registered in order, each as its own library.
		libraries func(ctrl *gomock.Controller) []testLibrary
		expected  []string
	}{
		{
			name: "concrete component type",
			libraries: func(ctrl *gomock.Controller) []testLibrary {
				return []testLibrary{{components: components(), tasks: []*RegistrableTask{
					NewRegistrableSideEffectTask(
						"task",
						NewMockSideEffectTaskHandler[*TestSubComponent1, *TestSideEffectTask](ctrl),
						WithTaskCountMetric(0),
					),
				}}}
			},
			expected: []string{"sub1"},
		},
		{
			name: "interface component type",
			libraries: func(ctrl *gomock.Controller) []testLibrary {
				return []testLibrary{{components: components(), tasks: []*RegistrableTask{
					NewRegistrableSideEffectTask(
						"task",
						NewMockSideEffectTaskHandler[any, *TestSideEffectTask](ctrl),
						WithTaskCountMetric(0),
					),
				}}}
			},
			expected: []string{"sub1", "sub11"},
		},
		{
			name: "task registered before its component",
			libraries: func(ctrl *gomock.Controller) []testLibrary {
				return []testLibrary{
					{tasks: []*RegistrableTask{
						NewRegistrableSideEffectTask(
							"task",
							NewMockSideEffectTaskHandler[*TestSubComponent1, *TestSideEffectTask](ctrl),
							WithTaskCountMetric(0),
						),
					}},
					{components: components()},
				}
			},
			expected: []string{"sub1"},
		},
		{
			name: "not opted in",
			libraries: func(ctrl *gomock.Controller) []testLibrary {
				return []testLibrary{{components: components(), tasks: []*RegistrableTask{
					NewRegistrableSideEffectTask(
						"task",
						NewMockSideEffectTaskHandler[*TestSubComponent1, *TestSideEffectTask](ctrl),
					),
				}}}
			},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			registry := NewRegistry(log.NewNoopLogger())
			for i, lib := range tc.libraries(ctrl) {
				mockLib := NewMockLibrary(ctrl)
				mockLib.EXPECT().Name().Return([]string{"libA", "libB"}[i]).AnyTimes()
				mockLib.EXPECT().Components().Return(lib.components)
				mockLib.EXPECT().Tasks().Return(lib.tasks)
				mockLib.EXPECT().NexusServices().Return(nil)
				mockLib.EXPECT().NexusServiceProcessors().Return(nil)
				require.NoError(t, registry.Register(mockLib))
			}

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
