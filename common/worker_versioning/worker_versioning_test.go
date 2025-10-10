package worker_versioning

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
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
)

func TestCalculateTaskQueueVersioningInfo(t *testing.T) {
	t1 := timestamp.TimePtr(time.Now().Add(-2 * time.Hour))
	t2 := timestamp.TimePtr(time.Now().Add(-time.Hour))
	t3 := timestamp.TimePtr(time.Now())

	tests := []struct {
		name        string
		wantCurrent *deploymentspb.DeploymentVersionData
		wantRamping *deploymentspb.DeploymentVersionData
		data        *persistencespb.DeploymentData
	}{
		{name: "nil data"},
		{name: "empty data", data: &persistencespb.DeploymentData{}},
		{name: "old data", wantCurrent: &deploymentspb.DeploymentVersionData{Version: v1, RoutingUpdateTime: t1},
			data: &persistencespb.DeploymentData{
				Deployments: []*persistencespb.DeploymentData_DeploymentDataItem{
					{Deployment: DeploymentFromDeploymentVersion(v1), Data: &deploymentspb.TaskQueueData{LastBecameCurrentTime: t1}},
				}},
		},
		{name: "old and new data", wantCurrent: &deploymentspb.DeploymentVersionData{Version: v2, RoutingUpdateTime: t2, CurrentSinceTime: t2},
			data: &persistencespb.DeploymentData{
				Deployments: []*persistencespb.DeploymentData_DeploymentDataItem{
					{Deployment: DeploymentFromDeploymentVersion(v1), Data: &deploymentspb.TaskQueueData{LastBecameCurrentTime: t1}},
				},
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v2, CurrentSinceTime: t2, RoutingUpdateTime: t2},
				},
			},
		},
		{name: "two current + two ramping",
			wantCurrent: &deploymentspb.DeploymentVersionData{Version: v2, RoutingUpdateTime: t2, CurrentSinceTime: t2},
			wantRamping: &deploymentspb.DeploymentVersionData{Version: v3, RoutingUpdateTime: t3, RampPercentage: 20, RampingSinceTime: t1},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, CurrentSinceTime: t1, RoutingUpdateTime: t1},
					{Version: v2, CurrentSinceTime: t2, RoutingUpdateTime: t2},
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2, RampingSinceTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3, RampingSinceTime: t1},
				},
			},
		},
		{name: "ramp without current", wantRamping: &deploymentspb.DeploymentVersionData{Version: v3, RoutingUpdateTime: t3, RampPercentage: 20, RampingSinceTime: t3},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t2, RampingSinceTime: t2},
					{Version: v3, RampPercentage: 20, RoutingUpdateTime: t3, RampingSinceTime: t3},
				},
			},
		},
		{name: "ramp to unversioned",
			wantRamping: &deploymentspb.DeploymentVersionData{Version: nil, RoutingUpdateTime: t2, RampPercentage: 20, RampingSinceTime: t2},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RampPercentage: 50, RoutingUpdateTime: t1, RampingSinceTime: t1},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 20, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "ramp 100%",
			wantCurrent: &deploymentspb.DeploymentVersionData{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
			wantRamping: &deploymentspb.DeploymentVersionData{Version: v2, RoutingUpdateTime: t2, RampPercentage: 100, RampingSinceTime: t2},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
					{Version: v2, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
				},
			},
		},
		{name: "ramp to unversioned 100%",
			wantCurrent: &deploymentspb.DeploymentVersionData{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
			wantRamping: &deploymentspb.DeploymentVersionData{Version: nil, RoutingUpdateTime: t2, RampPercentage: 100, RampingSinceTime: t2},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: t1},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
		{name: "ramp to unversioned 100% without current",
			wantCurrent: nil,
			wantRamping: &deploymentspb.DeploymentVersionData{Version: nil, RoutingUpdateTime: t2, RampPercentage: 100, RampingSinceTime: t2},
			data: &persistencespb.DeploymentData{
				Versions: []*deploymentspb.DeploymentVersionData{
					{Version: v1, RoutingUpdateTime: t1, CurrentSinceTime: nil},
				},
				UnversionedRampData: &deploymentspb.DeploymentVersionData{Version: nil, RampPercentage: 100, RoutingUpdateTime: t2, RampingSinceTime: t2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current, ramping := CalculateTaskQueueVersioningInfo(tt.data)
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
		{name: "with current version", current: &deploymentspb.DeploymentVersionData{Version: v1}, want: v1},
		{name: "with full ramp", current: &deploymentspb.DeploymentVersionData{Version: v1}, ramping: &deploymentspb.DeploymentVersionData{Version: v2, RampPercentage: 100}, want: v2},
		{name: "with full ramp to unversioned", current: &deploymentspb.DeploymentVersionData{Version: v1}, ramping: &deploymentspb.DeploymentVersionData{RampPercentage: 100}, want: nil},
		{name: "with full ramp from unversioned", ramping: &deploymentspb.DeploymentVersionData{Version: v1, RampPercentage: 100}, want: v1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindDeploymentVersionForWorkflowID(tt.current, tt.ramping, "my-wf-id"); !got.Equal(tt.want) {
				t.Errorf("FindDeploymentVersionForWorkflowID() = %v, want %v", got, tt.want)
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
					Version: tt.from,
				}
			}
			ramping = &deploymentspb.DeploymentVersionData{
				Version:        tt.to,
				RampPercentage: 30,
			}
			histogram := make(map[string]int)
			runs := 1000000
			for i := 0; i < runs; i++ {
				v := FindDeploymentVersionForWorkflowID(current, ramping, "wf-"+strconv.Itoa(i))
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
			input:       WorkerDeploymentVersionIdDelimiter,
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
				assert.Error(t, err)
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
