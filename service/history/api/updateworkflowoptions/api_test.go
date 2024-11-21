// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package updateworkflowoptions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
)

var (
	emptyOptions            = &workflowpb.WorkflowExecutionOptions{}
	unpinnedOverrideOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	}
	pinnedOverrideOptionsA = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: &deploymentpb.Deployment{SeriesName: "X", BuildId: "A"},
		},
	}
	pinnedOverrideOptionsB = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:   enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment: &deploymentpb.Deployment{SeriesName: "X", BuildId: "B"},
		},
	}
)

func TestMergeOptions_VersionOverrideMask(t *testing.T) {
	paths := []string{"versioning_override"}
	opts := &workflowpb.WorkflowExecutionOptions{}

	// Merge unpinned into empty options
	opts, err := MergeOptions(paths, unpinnedOverrideOptions, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(unpinnedOverrideOptions, opts))

	// Merge pinned_A into unpinned options
	opts, err = MergeOptions(paths, pinnedOverrideOptionsA, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(pinnedOverrideOptionsA, opts))

	// Merge pinned_B into pinned_A options
	opts, err = MergeOptions(paths, pinnedOverrideOptionsB, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(pinnedOverrideOptionsB, opts))

	// Unset versioning override
	opts, err = MergeOptions(paths, emptyOptions, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(emptyOptions, opts))
}

func TestMergeOptions_PartialUpdateMask(t *testing.T) {
	bothPaths := []string{"versioning_override.behavior", "versioning_override.deployment"}
	behaviorOnlyPaths := []string{"versioning_override.behavior"}
	deploymentOnlyPaths := []string{"versioning_override.deployment"}
	_, err := MergeOptions(behaviorOnlyPaths, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
	_, err = MergeOptions(deploymentOnlyPaths, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
	opts, err := MergeOptions(bothPaths, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.NoError(t, err)
	assert.True(t, proto.Equal(unpinnedOverrideOptions, opts))
}

func TestMergeOptions_EmptyPathsMask(t *testing.T) {
	opts, err := MergeOptions([]string{}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.NoError(t, err)
	assert.True(t, proto.Equal(emptyOptions, opts))
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	_, err := MergeOptions([]string{"*"}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	_, err := MergeOptions([]string{"foo"}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
}
