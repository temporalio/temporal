package worker_versioning

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.uber.org/mock/gomock"
)

var (
	v1 = &deploymentspb.WorkerDeploymentVersion{
		BuildId:        "v1",
		DeploymentName: "foo",
	}
	v2 = &deploymentspb.WorkerDeploymentVersion{
		BuildId:        "v2",
		DeploymentName: "foo",
	}
	v3 = &deploymentspb.WorkerDeploymentVersion{
		BuildId:        "v3",
		DeploymentName: "foo",
	}
	v4 = &deploymentspb.WorkerDeploymentVersion{
		BuildId:        "v4",
		DeploymentName: "bar",
	}
)

func TestCalculateTaskQueueVersioningInfo(t *testing.T) {
	t1 := timestamp.TimePtr(time.Now().Add(-2 * time.Hour))
	t2 := timestamp.TimePtr(time.Now().Add(-time.Hour))
	t3 := timestamp.TimePtr(time.Now())

	tests := []struct {
		name        string
		wantCurrent *deploymentspb.WorkerDeploymentVersion
		wantRamping *deploymentspb.WorkerDeploymentVersion
		data        *persistencespb.DeploymentData
	}{
		{name: "nil data"},
		{name: "empty data", data: &persistencespb.DeploymentData{}},
		{name: "old data", wantCurrent: v1,
			data: &persistencespb.DeploymentData{
				Deployments: []*persistencespb.DeploymentData_DeploymentDataItem{
					{Deployment: DeploymentFromDeploymentVersion(v1), Data: &deploymentspb.TaskQueueData{LastBecameCurrentTime: t1}},
				}},
		},
		{name: "old deployment data: two current + two ramping",
			wantCurrent: v2,
			wantRamping: v3,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, CurrentSinceTime: t1, RoutingUpdateTime: t1},
					{Version: v2, CurrentSinceTime: t2, RoutingUpdateTime: t2},
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2, RampingSinceTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3, RampingSinceTime: t1},
				},
			},
		},
		{name: "old deployment data: ramp without current", wantRamping: v3,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2, RampingSinceTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3, RampingSinceTime: t3},
				},
			},
		},
		{name: "old deployment data: ramp to unversioned",
			wantRamping: nil,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t1, RampingSinceTime: t1},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 20, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "old deployment data: ramp 100%",
			wantCurrent: v1,
			wantRamping: v2,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
					{Version: v2, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
				},
			},
		},
		{name: "old deployment data: ramp to unversioned 100%",
			wantCurrent: v1,
			wantRamping: nil,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "old deployment data: ramp to unversioned 100% without current",
			wantCurrent: nil,
			wantRamping: nil,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: nil},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "mix of prerelease and public preview deployment data: one current", wantCurrent: v2,
			data: &persistencespb.DeploymentData{
				Deployments: []*persistencespb.DeploymentData_DeploymentDataItem{
					{Deployment: DeploymentFromDeploymentVersion(v1), Data: &deploymentspb.TaskQueueData{LastBecameCurrentTime: t1}},
				},
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v2, CurrentSinceTime: t2, RoutingUpdateTime: t2},
				},
			},
		},
		// Membership related tests
		{name: "mixed: new RoutingConfig current overrides old when newer in membership", wantCurrent: v2,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					// Old format: v1 is current at older time t1
					{Version: v1, CurrentSinceTime: t1, RoutingUpdateTime: t1},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					v2.GetDeploymentName(): {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
								DeploymentName: v2.GetDeploymentName(),
								BuildId:        v2.GetBuildId(),
							},
							CurrentVersionChangedTime: t2,
						},
						// Membership contains v2 so HasDeploymentVersion() passes
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v2.GetBuildId(): {},
						},
					},
				},
			},
		},
		{name: "mixed: fall back to old current when new current not in membership", wantCurrent: v1,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					// Old format: v1 is current at older time t1
					{Version: v1, CurrentSinceTime: t1, RoutingUpdateTime: t1},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					v2.GetDeploymentName(): {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
								DeploymentName: v2.GetDeploymentName(),
								BuildId:        v2.GetBuildId(),
							},
							CurrentVersionChangedTime: t2,
						},
						// Membership missing v2 -> new format should be ignored for current
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{},
					},
				},
			},
		},
		{name: "mixed: new RoutingConfig ramping overrides old when newer in membership", wantRamping: v3,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					// Old format: v2 is ramping at older time t1
					{Version: v2, RampingSinceTime: t1, RoutingUpdateTime: t1, RampPercentage: 30},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					v3.GetDeploymentName(): {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
								DeploymentName: v3.GetDeploymentName(),
								BuildId:        v3.GetBuildId(),
							},
							RampingVersionPercentage:            20,
							RampingVersionPercentageChangedTime: t2,
						},
						// Membership contains v3 so HasDeploymentVersion() passes
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v3.GetBuildId(): {},
						},
					},
				},
			},
		},
		{name: "mixed: fall back to old ramping when new ramping not in membership", wantRamping: v2,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					// Old format: v2 is ramping at older time t1
					{Version: v2, RampingSinceTime: t1, RoutingUpdateTime: t1, RampPercentage: 30},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					v3.GetDeploymentName(): {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
								DeploymentName: v3.GetDeploymentName(),
								BuildId:        v3.GetBuildId(),
							},
							RampingVersionPercentage:            20,
							RampingVersionPercentageChangedTime: t2,
						},
						// Membership missing v3 -> new format should be ignored for ramping
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{},
					},
				},
			},
		},
		{name: "mixed: unversioned current newer than older current version -> keep old versioned", wantCurrent: v4,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					// Old format: v4 is current at older time t1
					{Version: v4, CurrentSinceTime: t1, RoutingUpdateTime: t1},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					// New format: sets current to unversioned at newer time t3
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  nil, // unversioned
							CurrentVersionChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							// Membership irrelevant for unversioned; keep empty or arbitrary
						},
					},
				},
			},
		},
		{name: "mixed: unversioned current without any other current version -> unversioned", wantCurrent: nil,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  nil, // unversioned
							CurrentVersionChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							// Membership irrelevant for unversioned; keep empty or arbitrary
						},
					},
				},
			},
		},
		{name: "mixed: unversioned ramping newer than older ramping version -> keep old versioned", wantRamping: v4,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					// Old format: v4 is ramping at older time t1
					{Version: v4, RampingSinceTime: t1, RoutingUpdateTime: t1, RampPercentage: 30},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion:            nil, // unversioned ramp target
							RampingVersionPercentage:            20,
							RampingVersionPercentageChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{},
					},
				},
			},
		},
		{name: "mixed: unversioned ramping without any other ramping version -> unversioned", wantRamping: nil,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion:            nil, // unversioned ramp target
							RampingVersionPercentage:            25,
							RampingVersionPercentageChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{},
					},
				},
			},
		},
		{name: "new format: unversioned current with newer timestamp with another current version in a different deployment -> current is still versioned", wantCurrent: v1,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: "v1"},
							CurrentVersionChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {},
						},
					},
					"bar": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  nil,
							CurrentVersionChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{},
					},
				},
			}},
		{name: "new format: unversioned ramping with newer timestamp with another ramping version in a different deployment -> ramping is still versioned", wantRamping: v1,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v1.GetBuildId()},
							RampingVersionPercentage:            30,
							RampingVersionPercentageChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {},
						},
					},
					"bar": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion:            nil,
							RampingVersionPercentage:            20,
							RampingVersionPercentageChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{},
					},
				},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current, _, _, ramping, _, _, _, _ := CalculateTaskQueueVersioningInfo(tt.data)
			if !current.Equal(tt.wantCurrent) {
				t.Errorf("got current = %v, want %v", current, tt.wantCurrent)
			}
			if !ramping.Equal(tt.wantRamping) {
				t.Errorf("got ramping = %v, want %v", ramping, tt.wantRamping)
			}
		})
	}
}

func TestFindDeploymentVersionForWorkflowID(t *testing.T) {
	tests := []struct {
		name    string
		current *deploymentspb.DeploymentVersionData
		ramping *deploymentspb.DeploymentVersionData
		want    *deploymentspb.WorkerDeploymentVersion
	}{
		{name: "nil current and ramping info", want: nil},
		{name: "with current version", current: &deploymentspb.DeploymentVersionData{Version: v1, RoutingUpdateTime: timestamp.TimePtr(time.Now())}, want: v1},
		{name: "with full ramp", current: &deploymentspb.DeploymentVersionData{Version: v1, RoutingUpdateTime: timestamp.TimePtr(time.Now())}, ramping: &deploymentspb.DeploymentVersionData{Version: v2, RampPercentage: 100, RoutingUpdateTime: timestamp.TimePtr(time.Now())}, want: v2},
		{name: "with full ramp to unversioned", current: &deploymentspb.DeploymentVersionData{Version: v1, RoutingUpdateTime: timestamp.TimePtr(time.Now())}, ramping: &deploymentspb.DeploymentVersionData{RampPercentage: 100, RoutingUpdateTime: timestamp.TimePtr(time.Now())}, want: nil},
		{name: "with full ramp from unversioned", ramping: &deploymentspb.DeploymentVersionData{Version: v1, RampPercentage: 100, RoutingUpdateTime: timestamp.TimePtr(time.Now())}, want: v1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(tt.current.GetVersion(), 0, tt.ramping.GetVersion(), tt.ramping.GetRampPercentage(), 0, "my-wf-id"); !got.Equal(tt.want) {
				t.Errorf("FindTargetDeploymentVersionAndRevisionNumberForWorkflowID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindDeploymentVersionForWorkflowID_PartialRamp(t *testing.T) {
	tests := []struct {
		name string
		from *deploymentspb.WorkerDeploymentVersion
		to   *deploymentspb.WorkerDeploymentVersion
	}{
		{name: "from v1 to v2", from: v1, to: v2},
		{name: "from v1 to unversioned", from: v1},
		{name: "from unversioned to v2", to: v2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var current *deploymentspb.DeploymentVersionData
			var ramping *deploymentspb.DeploymentVersionData
			if tt.from != nil {
				current = &deploymentspb.DeploymentVersionData{
					Version:           tt.from,
					RoutingUpdateTime: timestamp.TimePtr(time.Now()),
				}
			}
			ramping = &deploymentspb.DeploymentVersionData{
				Version:           tt.to,
				RampPercentage:    30,
				RoutingUpdateTime: timestamp.TimePtr(time.Now()),
			}
			histogram := make(map[string]int)
			runs := 1000000
			for i := 0; i < runs; i++ {
				v, _ := FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(current.GetVersion(), 0, ramping.GetVersion(), ramping.GetRampPercentage(), 0, "wf-"+strconv.Itoa(i))
				histogram[v.GetBuildId()]++
			}

			assert.InEpsilon(t, .7*float64(runs), histogram[tt.from.GetBuildId()], .02)
			assert.InEpsilon(t, .3*float64(runs), histogram[tt.to.GetBuildId()], .02)
		})
	}
}

func TestWorkerDeploymentVersionFromStringV32(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    *deploymentspb.WorkerDeploymentVersion
		expectedErr string
	}{
		{
			name:  "valid version",
			input: "my-deployment:build-123",
			expected: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: "my-deployment",
				BuildId:        "build-123",
			},
		},
		{
			name:  "multiple delimiters",
			input: "my-deployment:build-123:extra",
			expected: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: "my-deployment",
				BuildId:        "build-123:extra",
			},
		},
		{
			name:     "skip unversioned",
			input:    UnversionedVersionId,
			expected: nil,
		},
		{
			name:        "empty string",
			input:       "",
			expectedErr: "expected delimiter ':' not found in version string ",
		},
		{
			name:        "only delimiter",
			input:       WorkerDeploymentVersionWorkflowIDDelimeter,
			expectedErr: "deployment name is empty in version string :",
		},
		{
			name:        "missing delimiter",
			input:       "my-deployment-build-123",
			expectedErr: "expected delimiter ':' not found in version string my-deployment-build-123",
		},
		{
			name:        "empty deployment name",
			input:       ":build-123",
			expectedErr: "deployment name is empty in version string :build-123",
		},
		{
			name:        "empty build id",
			input:       "my-deployment:",
			expectedErr: "build id is empty in version string my-deployment:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WorkerDeploymentVersionFromStringV32(tt.input)
			if tt.expectedErr != "" {
				assert.NotNil(t, err)
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestValidateVersioningOverride(t *testing.T) {
	testNamespaceID := "test-namespace-id"
	testTaskQueue := &taskqueuepb.TaskQueue{Name: "test-task-queue"}
	testVersion := &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: "test-deployment",
		BuildId:        "test-build-id",
	}

	tests := []struct {
		name          string
		override      *workflowpb.VersioningOverride
		setupCache    func(c cache.Cache)
		setupMock     func(m *matchingservicemock.MockMatchingServiceClient)
		expectError   bool
		errorContains string
	}{
		{
			name:        "nil override returns nil",
			override:    nil,
			setupCache:  func(c cache.Cache) {},
			setupMock:   func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError: false,
		},
		{
			name: "v0.32: AutoUpgrade override returns nil",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
			},
			setupCache: func(c cache.Cache) {},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0) // No RPC call expected!
			},
			expectError: false,
		},
		{
			name: "v0.32: Pinned override, with cache hit, returns nil",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  testVersion,
					},
				},
			},
			setupCache: func(c cache.Cache) {
				key := testNamespaceID + ":" + testTaskQueue.Name + ":" + testVersion.DeploymentName + ":" + testVersion.BuildId
				c.Put(key, true)
			},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0) // No RPC call expected!
			},
			expectError: false,
		},
		{
			name: "v0.32: Pinned override, with cache hit, returns error (since version is not present in the task queue)",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  testVersion,
					},
				},
			},
			setupCache: func(c cache.Cache) {
				key := testNamespaceID + ":" + testTaskQueue.Name + ":" + testVersion.DeploymentName + ":" + testVersion.BuildId
				c.Put(key, false)
			},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0) // No RPC call expected!
			},
			expectError:   true,
			errorContains: "Pinned version is not present in the task queue",
		},
		{
			name: "v0.32: Pinned override, with cache miss, calls RPC and caches false",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  testVersion,
					},
				},
			},
			setupCache: func(c cache.Cache) {},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(
					gomock.Any(),
					gomock.Any(),
				).Return(&matchingservice.CheckTaskQueueVersionMembershipResponse{
					IsMember: false,
				}, nil)
			},
			expectError:   true,
			errorContains: "Pinned version is not present in the task queue",
		},
		{
			name: "v0.32: Pinned override, with cache miss, calls RPC and caches true",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  testVersion,
					},
				},
			},
			setupCache: func(c cache.Cache) {},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(
					gomock.Any(),
					gomock.Any(),
				).Return(&matchingservice.CheckTaskQueueVersionMembershipResponse{
					IsMember: true,
				}, nil)
			},
			expectError: false,
		},
		{
			name: "v0.32: Pinned override, without version, returns error",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  nil,
					},
				},
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "must provide version if override is pinned",
		},
		{
			name: "v0.32: Pinned override, without behavior, returns error",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_UNSPECIFIED,
						Version:  testVersion,
					},
				},
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "must specify pinned override behavior if override is pinned",
		},
		// v0.31 tests (deprecated behavior field)
		{
			name: "v0.31: AUTO_UPGRADE behavior returns nil",
			override: &workflowpb.VersioningOverride{
				Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
			},
			setupCache: func(c cache.Cache) {},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0)
			},
			expectError: false,
		},
		{
			name: "v0.31: AUTO_UPGRADE with deployment set returns error",
			override: &workflowpb.VersioningOverride{
				Behavior:   enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
				Deployment: &deploymentpb.Deployment{SeriesName: "test", BuildId: "build1"},
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "only provide deployment if behavior is 'PINNED'",
		},
		{
			name: "v0.31: AUTO_UPGRADE with pinned_version set returns error",
			override: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
				PinnedVersion: "test-deployment.test-build-id",
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "only provide pinned version if behavior is 'PINNED'",
		},
		{
			name: "v0.31: PINNED behavior with pinned_version, cache hit, returns nil",
			override: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
				PinnedVersion: "test-deployment.test-build-id",
			},
			setupCache: func(c cache.Cache) {
				key := testNamespaceID + ":" + testTaskQueue.Name + ":test-deployment:test-build-id"
				c.Put(key, true)
			},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0)
			},
			expectError: false,
		},
		{
			name: "v0.31: PINNED behavior with pinned_version, cache hit, returns error (since version is not present in the task queue)",
			override: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
				PinnedVersion: "test-deployment.test-build-id",
			},
			setupCache: func(c cache.Cache) {
				key := testNamespaceID + ":" + testTaskQueue.Name + ":test-deployment:test-build-id"
				c.Put(key, false)
			},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0)
			},
			expectError:   true,
			errorContains: "Pinned version is not present in the task queue",
		},
		{
			name: "v0.31: PINNED behavior with pinned_version, cache miss, calls RPC and caches false",
			override: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
				PinnedVersion: "test-deployment.test-build-id",
			},
			setupCache: func(c cache.Cache) {},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(
					gomock.Any(),
					gomock.Any(),
				).Return(&matchingservice.CheckTaskQueueVersionMembershipResponse{
					IsMember: false,
				}, nil)
			},
			expectError:   true,
			errorContains: "Pinned version is not present in the task queue",
		},
		{
			name: "v0.31: PINNED behavior with pinned_version, cache miss, calls RPC and caches true",
			override: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
				PinnedVersion: "test-deployment.test-build-id",
			},
			setupCache: func(c cache.Cache) {},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(
					gomock.Any(),
					gomock.Any(),
				).Return(&matchingservice.CheckTaskQueueVersionMembershipResponse{
					IsMember: true,
				}, nil)
			},
			expectError: false,
		},
		{
			name: "v0.31: PINNED behavior without deployment or pinned_version returns error",
			override: &workflowpb.VersioningOverride{
				Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "must provide deployment (deprecated) or pinned version if behavior is 'PINNED'",
		},
		{
			name: "v0.31: PINNED behavior with invalid pinned_version format returns error",
			override: &workflowpb.VersioningOverride{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
				PinnedVersion: "invalid-no-dot",
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "invalid version string",
		},
		{
			name: "v0.31: UNSPECIFIED behavior returns error",
			override: &workflowpb.VersioningOverride{
				Behavior: enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED,
			},
			setupCache:    func(c cache.Cache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "override behavior is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			tt.setupMock(mockMatchingClient)

			testCache := cache.New(100, &cache.Options{
				TTL: time.Minute,
			})
			tt.setupCache(testCache)

			err := ValidateVersioningOverride(
				tt.override,
				mockMatchingClient,
				testCache,
				testTaskQueue,
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				testNamespaceID,
			)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
