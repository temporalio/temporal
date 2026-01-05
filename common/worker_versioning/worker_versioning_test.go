package worker_versioning

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

type testVersionMembershipCache struct {
	mu sync.Mutex
	m  map[versionMembershipCacheKey]bool
}

func newTestVersionMembershipCache() *testVersionMembershipCache {
	return &testVersionMembershipCache{m: make(map[versionMembershipCacheKey]bool)}
}

var _ VersionMembershipCache = (*testVersionMembershipCache)(nil)

func (c *testVersionMembershipCache) Get(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
) (isMember bool, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[versionMembershipCacheKey{
		namespaceID:    namespaceID,
		taskQueue:      taskQueue,
		taskQueueType:  taskQueueType,
		deploymentName: deploymentName,
		buildID:        buildID,
	}]
	return v, ok
}

func (c *testVersionMembershipCache) Put(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
	isMember bool,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[versionMembershipCacheKey{
		namespaceID:    namespaceID,
		taskQueue:      taskQueue,
		taskQueueType:  taskQueueType,
		deploymentName: deploymentName,
		buildID:        buildID,
	}] = isMember
}

func TestCalculateTaskQueueVersioningInfo(t *testing.T) {
	t1 := timestamp.TimePtr(time.Now().Add(-2 * time.Hour))
	t2 := timestamp.TimePtr(time.Now().Add(-time.Hour))
	t3 := timestamp.TimePtr(time.Now())

	tests := []struct {
		name               string
		wantCurrent        *deploymentspb.WorkerDeploymentVersion
		wantRamping        *deploymentspb.WorkerDeploymentVersion
		wantRampPercentage float32
		data               *persistencespb.DeploymentData
	}{
		{name: "nil data"},
		{name: "empty data", data: &persistencespb.DeploymentData{}},
		{name: "old deployment data: two current + two ramping",
			wantCurrent:        v2,
			wantRamping:        v3,
			wantRampPercentage: 20,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, CurrentSinceTime: t1, RoutingUpdateTime: t1},
					{Version: v2, CurrentSinceTime: t2, RoutingUpdateTime: t2},
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2, RampingSinceTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3, RampingSinceTime: t1},
				},
			},
		},
		{name: "old deployment data: ramp without current", wantRamping: v3, wantRampPercentage: 20,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2, RampingSinceTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3, RampingSinceTime: t3},
				},
			},
		},
		{name: "old deployment data: ramp to unversioned",
			wantRamping:        nil,
			wantRampPercentage: 20,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t1, RampingSinceTime: t1},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 20, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "old deployment data: ramp 100%",
			wantCurrent:        v1,
			wantRamping:        v2,
			wantRampPercentage: 100,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
					{Version: v2, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
				},
			},
		},
		{name: "old deployment data: ramp to unversioned 100%",
			wantCurrent:        v1,
			wantRamping:        nil,
			wantRampPercentage: 100,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "old deployment data: ramp to unversioned 100% without current",
			wantCurrent:        nil,
			wantRamping:        nil,
			wantRampPercentage: 100,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: nil},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		// Membership related tests
		{name: "mixed: new RoutingConfig current overrides old when newer in membership",
			wantCurrent: v2,
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
		{name: "mixed: fall back to old current when new current not in membership",
			wantCurrent: v1,
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
		{name: "mixed: new RoutingConfig ramping overrides old when newer in membership",
			wantRamping:        v3,
			wantRampPercentage: 20,
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
		{name: "mixed: fall back to old ramping when new ramping not in membership",
			wantRamping:        v2,
			wantRampPercentage: 30,
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
		{name: "mixed: unversioned current newer than older current version -> keep old versioned",
			wantCurrent: v4,
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
		{name: "mixed: unversioned current without any other current version -> unversioned",
			wantCurrent: nil,
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
		{name: "mixed: unversioned ramping newer than older ramping version -> keep old versioned",
			wantRamping:        v4,
			wantRampPercentage: 30,
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
		{name: "mixed: unversioned ramping without any other ramping version -> unversioned",
			wantRamping:        nil,
			wantRampPercentage: 25,
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
		{name: "new format: unversioned current with newer timestamp with another current version in a different deployment -> current is still versioned",
			wantCurrent: v1,
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
		{name: "new format: ramping to unversioned ",
			wantCurrent:        v1,
			wantRampPercentage: 20,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v1.GetBuildId()},
							CurrentVersionChangedTime:           t2,
							RampingDeploymentVersion:            nil,
							RampingVersionPercentage:            20,
							RampingVersionPercentageChangedTime: t3,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {},
						},
					},
				},
			}},
		{name: "new format: unversioned ramping with newer timestamp with another ramping version in a different deployment -> ramping is still versioned",
			wantRamping:        v1,
			wantRampPercentage: 30,
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
		{name: "new format: versioned current with unversioned ramping in same deployment -> current is versioned and ramping is unversioned", wantCurrent: v1, wantRamping: nil, wantRampPercentage: 20,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
								DeploymentName: v1.GetDeploymentName(),
								BuildId:        v1.GetBuildId(),
							},
							CurrentVersionChangedTime: t2,
							RampingDeploymentVersion:  nil, // unversioned ramp target
							RampingVersionPercentage:  20,
							// Use a newer timestamp so the unversioned ramping is picked from the new format.
							RampingVersionPercentageChangedTime: t3,
						},
						// Membership contains v1 so HasDeploymentVersion() passes for current.
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {},
						},
					},
				},
			}},
		// Tests with deleted versions
		{name: "new format: current version marked as deleted should be ignored",
			wantCurrent: nil,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v1.GetBuildId()},
							CurrentVersionChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
		{name: "new format: ramping version marked as deleted should be ignored",
			wantRamping: nil,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							RampingVersionPercentage:            50,
							RampingVersionPercentageChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v2.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
		{name: "new format: current deleted, ramping not deleted -> only ramping returned",
			wantCurrent:        nil,
			wantRamping:        v2,
			wantRampPercentage: 30,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v1.GetBuildId()},
							CurrentVersionChangedTime:           t1,
							RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							RampingVersionPercentage:            30,
							RampingVersionPercentageChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {Deleted: true},
							v2.GetBuildId(): {Deleted: false},
						},
					},
				},
			}},
		{name: "new format: ramping deleted, current not deleted -> only current returned",
			wantCurrent:        v1,
			wantRamping:        nil,
			wantRampPercentage: 0,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v1.GetBuildId()},
							CurrentVersionChangedTime:           t1,
							RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							RampingVersionPercentage:            30,
							RampingVersionPercentageChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {Deleted: false},
							v2.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
		{name: "new format: both current and ramping deleted -> both nil",
			wantCurrent:        nil,
			wantRamping:        nil,
			wantRampPercentage: 0,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v1.GetBuildId()},
							CurrentVersionChangedTime:           t1,
							RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							RampingVersionPercentage:            30,
							RampingVersionPercentageChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {Deleted: true},
							v2.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
		{name: "mixed: new current deleted falls back to old current",
			wantCurrent: v1,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, CurrentSinceTime: t1, RoutingUpdateTime: t1},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							CurrentVersionChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v2.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
		{name: "mixed: new ramping deleted falls back to old ramping",
			wantRamping:        v1,
			wantRampPercentage: 40,
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampingSinceTime: t1, RoutingUpdateTime: t1, RampPercentage: 40},
				},
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							RampingVersionPercentage:            50,
							RampingVersionPercentageChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v2.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
		{name: "new format: version exists but marked as deleted alongside non-deleted version",
			wantCurrent: v2,
			data: &persistencespb.DeploymentData{
				DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
					"foo": {
						RoutingConfig: &deploymentpb.RoutingConfig{
							CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: v2.GetBuildId()},
							CurrentVersionChangedTime: t2,
						},
						Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
							v1.GetBuildId(): {Deleted: true},
							v2.GetBuildId(): {Deleted: false},
							v3.GetBuildId(): {Deleted: true},
						},
					},
				},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current, _, _, ramping, _, rampPercentage, _, _ := CalculateTaskQueueVersioningInfo(tt.data)
			if !current.Equal(tt.wantCurrent) {
				t.Errorf("got current = %v, want %v", current, tt.wantCurrent)
			}
			if !ramping.Equal(tt.wantRamping) {
				t.Errorf("got ramping = %v, want %v", ramping, tt.wantRamping)
			}
			if rampPercentage != tt.wantRampPercentage {
				t.Errorf("got ramp percentage = %v, want %v", rampPercentage, tt.wantRampPercentage)
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
			input:       WorkerDeploymentVersionDelimiter,
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
	testTaskQueue := "test-task-queue"
	testVersion := &deploymentpb.WorkerDeploymentVersion{
		DeploymentName: "test-deployment",
		BuildId:        "test-build-id",
	}

	tests := []struct {
		name          string
		override      *workflowpb.VersioningOverride
		taskQueueType enumspb.TaskQueueType
		setupCache    func(c *testVersionMembershipCache)
		setupMock     func(m *matchingservicemock.MockMatchingServiceClient)
		expectError   bool
		errorContains string
	}{
		{
			name:        "nil override returns nil",
			override:    nil,
			setupCache:  func(c *testVersionMembershipCache) {},
			setupMock:   func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError: false,
		},
		{
			name: "v0.32: AutoUpgrade override returns nil",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
			},
			setupCache: func(c *testVersionMembershipCache) {},
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
			setupCache: func(c *testVersionMembershipCache) {
				c.Put(testNamespaceID, testTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, testVersion.DeploymentName, testVersion.BuildId, true)
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
			setupCache: func(c *testVersionMembershipCache) {
				c.Put(testNamespaceID, testTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, testVersion.DeploymentName, testVersion.BuildId, false)
			},
			setupMock: func(m *matchingservicemock.MockMatchingServiceClient) {
				m.EXPECT().CheckTaskQueueVersionMembership(gomock.Any(), gomock.Any()).Times(0) // No RPC call expected!
			},
			expectError:   true,
			errorContains: "Pinned version is not present in the task queue",
		},
		{
			name:          "v0.32: Pinned override, cache hit for different task queue type does not apply",
			taskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  testVersion,
					},
				},
			},
			setupCache: func(c *testVersionMembershipCache) {
				c.Put(testNamespaceID, testTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, testVersion.DeploymentName, testVersion.BuildId, true)
			},
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
			name: "v0.32: Pinned override, with cache miss, calls RPC and caches false",
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  testVersion,
					},
				},
			},
			setupCache: func(c *testVersionMembershipCache) {},
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
			setupCache: func(c *testVersionMembershipCache) {},
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
			setupCache:    func(c *testVersionMembershipCache) {},
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
			setupCache:    func(c *testVersionMembershipCache) {},
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
			setupCache: func(c *testVersionMembershipCache) {},
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
			setupCache:    func(c *testVersionMembershipCache) {},
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
			setupCache:    func(c *testVersionMembershipCache) {},
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
			setupCache: func(c *testVersionMembershipCache) {
				c.Put(testNamespaceID, testTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "test-deployment", "test-build-id", true)
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
			setupCache: func(c *testVersionMembershipCache) {
				c.Put(testNamespaceID, testTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, "test-deployment", "test-build-id", false)
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
			setupCache: func(c *testVersionMembershipCache) {},
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
			setupCache: func(c *testVersionMembershipCache) {},
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
			setupCache:    func(c *testVersionMembershipCache) {},
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
			setupCache:    func(c *testVersionMembershipCache) {},
			setupMock:     func(m *matchingservicemock.MockMatchingServiceClient) {},
			expectError:   true,
			errorContains: "invalid version string",
		},
		{
			name: "v0.31: UNSPECIFIED behavior returns error",
			override: &workflowpb.VersioningOverride{
				Behavior: enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED,
			},
			setupCache:    func(c *testVersionMembershipCache) {},
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

			testCache := newTestVersionMembershipCache()
			tt.setupCache(testCache)

			tqType := tt.taskQueueType
			if tqType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
				tqType = enumspb.TASK_QUEUE_TYPE_WORKFLOW
			}
			err := ValidateVersioningOverride(
				context.Background(),
				tt.override,
				mockMatchingClient,
				testCache,
				testTaskQueue,
				tqType,
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

func TestCleanupOldDeletedVersions(t *testing.T) {
	now := time.Now()
	eightDaysAgo := now.Add(-time.Hour * 24 * 8)
	sixDaysAgo := now.Add(-time.Hour * 24 * 6)
	fiveDaysAgo := now.Add(-time.Hour * 24 * 5)
	oneDayAgo := now.Add(-time.Hour * 24 * 1)

	tests := []struct {
		name        string
		versions    map[string]*deploymentspb.WorkerDeploymentVersionData
		maxVersions int
		wantCleaned bool
		wantRemoved []string
	}{
		{
			name: "removes only deleted versions older than 7 days when not exceeding limit",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: true, UpdateTime: timestamp.TimePtr(eightDaysAgo)},
				"v2": {Deleted: true, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
				"v3": {Deleted: false, UpdateTime: timestamp.TimePtr(oneDayAgo)},
			},
			maxVersions: 10,
			wantCleaned: true,
			wantRemoved: []string{"v1"},
		},
		{
			name: "removes deleted versions when exceeding limit due to deleted versions",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: false, UpdateTime: timestamp.TimePtr(eightDaysAgo)},
				"v2": {Deleted: false, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
				"v3": {Deleted: false, UpdateTime: timestamp.TimePtr(fiveDaysAgo)},
				"v4": {Deleted: true, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
				"v5": {Deleted: true, UpdateTime: timestamp.TimePtr(oneDayAgo)},
			},
			maxVersions: 3,
			wantCleaned: true,
			wantRemoved: []string{"v4", "v5"},
		},
		{
			name: "can exceed limit due to undeleted versions",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: false, UpdateTime: timestamp.TimePtr(eightDaysAgo)},
				"v2": {Deleted: false, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
				"v3": {Deleted: false, UpdateTime: timestamp.TimePtr(fiveDaysAgo)},
				"v4": {Deleted: false, UpdateTime: timestamp.TimePtr(oneDayAgo)},
				"v5": {Deleted: true, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
			},
			maxVersions: 3,
			wantCleaned: true,
			wantRemoved: []string{"v5"},
		},
		{
			name: "removes deleted versions from oldest to newest",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: false, UpdateTime: timestamp.TimePtr(now)},
				"v2": {Deleted: true, UpdateTime: timestamp.TimePtr(oneDayAgo)},
				"v3": {Deleted: true, UpdateTime: timestamp.TimePtr(fiveDaysAgo)},
				"v4": {Deleted: true, UpdateTime: timestamp.TimePtr(eightDaysAgo)},
			},
			maxVersions: 2,
			wantCleaned: true,
			wantRemoved: []string{"v4", "v3"},
		},
		{
			name: "stops when no deleted version older than 7 days and not exceeding limit",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: false, UpdateTime: timestamp.TimePtr(now)},
				"v2": {Deleted: false, UpdateTime: timestamp.TimePtr(oneDayAgo)},
				"v3": {Deleted: true, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
				"v4": {Deleted: true, UpdateTime: timestamp.TimePtr(oneDayAgo)},
			},
			maxVersions: 10,
			wantCleaned: false,
			wantRemoved: []string{},
		},
		{
			name: "removes nothing when all deleted versions are recent and within limit",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: true, UpdateTime: timestamp.TimePtr(oneDayAgo)},
				"v2": {Deleted: false, UpdateTime: timestamp.TimePtr(now)},
			},
			maxVersions: 10,
			wantCleaned: false,
			wantRemoved: []string{},
		},
		{
			name:        "handles empty versions map",
			versions:    map[string]*deploymentspb.WorkerDeploymentVersionData{},
			maxVersions: 10,
			wantCleaned: false,
			wantRemoved: []string{},
		},
		{
			name: "handles only undeleted versions",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: false, UpdateTime: timestamp.TimePtr(eightDaysAgo)},
				"v2": {Deleted: false, UpdateTime: timestamp.TimePtr(now)},
			},
			maxVersions: 10,
			wantCleaned: false,
			wantRemoved: []string{},
		},
		{
			name: "removes enough old deleted versions when significantly exceeding limit",
			versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
				"v1": {Deleted: false, UpdateTime: timestamp.TimePtr(now)},
				"d1": {Deleted: true, UpdateTime: timestamp.TimePtr(eightDaysAgo)},
				"d2": {Deleted: true, UpdateTime: timestamp.TimePtr(eightDaysAgo.Add(-time.Hour))},
				"d3": {Deleted: true, UpdateTime: timestamp.TimePtr(eightDaysAgo.Add(-time.Hour * 2))},
				"d4": {Deleted: true, UpdateTime: timestamp.TimePtr(eightDaysAgo.Add(-time.Hour * 3))},
				"d5": {Deleted: true, UpdateTime: timestamp.TimePtr(sixDaysAgo)},
			},
			maxVersions: 2,
			wantCleaned: true,
			wantRemoved: []string{"d4", "d3", "d2", "d1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deploymentData := &persistencespb.WorkerDeploymentData{
				Versions: tt.versions,
			}

			// Make a copy of the original keys to verify removals
			originalKeys := make(map[string]bool)
			for k := range tt.versions {
				originalKeys[k] = true
			}

			cleaned := CleanupOldDeletedVersions(deploymentData, tt.maxVersions)

			// Check if cleaned flag matches expectation
			assert.Equal(t, tt.wantCleaned, cleaned, "cleaned flag mismatch")

			// Check that expected versions were removed
			for _, removed := range tt.wantRemoved {
				_, exists := deploymentData.Versions[removed]
				assert.False(t, exists, "version %s should have been removed but still exists", removed)
			}

			// Check that only expected versions were removed
			for k := range originalKeys {
				_, exists := deploymentData.Versions[k]
				shouldBeRemoved := false
				for _, removed := range tt.wantRemoved {
					if k == removed {
						shouldBeRemoved = true
						break
					}
				}
				if shouldBeRemoved {
					assert.False(t, exists, "version %s should have been removed", k)
				} else {
					assert.True(t, exists, "version %s should not have been removed", k)
				}
			}

			// Verify invariants after cleanup
			deletedCount := 0
			undeletedCount := 0
			for _, v := range deploymentData.Versions {
				if v.GetDeleted() {
					deletedCount++
				} else {
					undeletedCount++
				}
			}

			// After cleanup, we should not exceed the limit because of deleted versions
			// (though we may exceed it due to undeleted versions, which is acceptable)
			if undeletedCount <= tt.maxVersions {
				// If undeleted count is within limit, check that remaining deleted versions
				// are either recent (< 30 days) or we're still within total limit
				for _, v := range deploymentData.Versions {
					if v.GetDeleted() {
						age := now.Sub(v.GetUpdateTime().AsTime())
						totalCount := undeletedCount + deletedCount
						if age >= time.Hour*24*30 {
							// Old deleted version should only remain if we're at or under the limit
							assert.LessOrEqual(t, totalCount, tt.maxVersions,
								"old deleted version remains but exceeding limit")
						}
					}
				}
			}
		})
	}
}
