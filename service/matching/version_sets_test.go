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

package matching

import (
	"fmt"
	"go.temporal.io/api/serviceerror"
	"testing"

	"github.com/stretchr/testify/assert"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	persistencepb "go.temporal.io/server/api/persistence/v1"
)

func mkNewSet(id string) *taskqueuepb.CompatibleVersionSet {
	return &taskqueuepb.CompatibleVersionSet{
		Versions: []string{id},
	}
}

func mkInitialData(numSets int) *persistencepb.VersioningData {
	sets := make([]*taskqueuepb.CompatibleVersionSet, numSets)
	for i := 0; i < numSets; i++ {
		sets[i] = mkNewSet(fmt.Sprintf("%v", i))
	}
	return &persistencepb.VersioningData{
		VersionSets: sets,
	}
}

func mkNewDefReq(id string) *workflowservice.UpdateWorkerBuildIdOrderingRequest {
	return &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdOrderingRequest_NewDefaultVersionId{
			NewDefaultVersionId: id,
		},
	}
}
func mkNewCompatReq(id, compat string, becomeDefault bool) *workflowservice.UpdateWorkerBuildIdOrderingRequest {
	return &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdOrderingRequest_NewCompatibleVersion_{
			NewCompatibleVersion: &workflowservice.UpdateWorkerBuildIdOrderingRequest_NewCompatibleVersion{
				NewVersionId:              id,
				ExistingCompatibleVersion: compat,
				BecomeDefault:             becomeDefault,
			},
		},
	}
}
func mkExistingDefault(id string) *workflowservice.UpdateWorkerBuildIdOrderingRequest {
	return &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Operation: &workflowservice.UpdateWorkerBuildIdOrderingRequest_ExistingVersionIdInSetToPromote{
			ExistingVersionIdInSetToPromote: id,
		},
	}
}

func TestNewDefaultUpdate(t *testing.T) {
	data := mkInitialData(2)

	req := mkNewDefReq("2")
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	curd := data.VersionSets[len(data.VersionSets)-1]
	assert.Equal(t, "2", curd.Versions[0])
	assert.Equal(t, "1", data.VersionSets[1].Versions[0])
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])

	asResp := ToBuildIdOrderingResponse(data, 0)
	assert.Equal(t, "2", asResp.MajorVersionSets[2].Versions[0])
}

func TestNewDefaultGraphUpdateOfEmptyGraph(t *testing.T) {
	data := &persistencepb.VersioningData{}

	req := mkNewDefReq("1")
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	curd := data.VersionSets[len(data.VersionSets)-1]
	assert.Equal(t, "1", curd.Versions[0])
	assert.Equal(t, 1, len(data.VersionSets))
}

func TestNewDefaultGraphUpdateCompatWithCurDefault(t *testing.T) {
	data := mkInitialData(2)

	req := mkNewCompatReq("1.1", "1", true)
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	curd := data.VersionSets[len(data.VersionSets)-1]
	assert.Equal(t, "1.1", curd.Versions[1])
	assert.Equal(t, "1", curd.Versions[0])
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])
}

func TestNewDefaultGraphUpdateCompatWithNonDefaultSet(t *testing.T) {
	data := mkInitialData(2)

	req := mkNewCompatReq("0.1", "0", true)
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	curd := data.VersionSets[len(data.VersionSets)-1]
	assert.Equal(t, "0.1", curd.Versions[1])
	assert.Equal(t, "0", curd.Versions[0])
	assert.Equal(t, "1", data.VersionSets[0].Versions[0])
}

func TestNewCompatibleWithVerInOlderSet(t *testing.T) {
	data := mkInitialData(3)

	req := mkNewCompatReq("0.1", "0", false)
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	curd := data.VersionSets[len(data.VersionSets)-1]
	assert.Equal(t, "2", curd.Versions[0])
	assert.Equal(t, "0.1", data.VersionSets[0].Versions[1])
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])

	asResp := ToBuildIdOrderingResponse(data, 0)
	assert.Equal(t, "0.1", asResp.MajorVersionSets[0].Versions[1])
}

func TestNewCompatibleWithNonDefaultGraphUpdate(t *testing.T) {
	data := mkInitialData(2)

	req := mkNewCompatReq("0.1", "0", false)
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	req = mkNewCompatReq("0.2", "0.1", false)
	err = UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	curd := data.VersionSets[len(data.VersionSets)-1]
	assert.Equal(t, "1", curd.Versions[0])
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])
	assert.Equal(t, "0.1", data.VersionSets[0].Versions[1])
	assert.Equal(t, "0.2", data.VersionSets[0].Versions[2])

	// Ensure setting a compatible version which targets a non-leaf compat version ends up without a branch
	req = mkNewCompatReq("0.3", "0.1", false)
	err = UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	assert.Equal(t, "1", curd.Versions[0])
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])
	assert.Equal(t, "0.1", data.VersionSets[0].Versions[1])
	assert.Equal(t, "0.2", data.VersionSets[0].Versions[2])
	assert.Equal(t, "0.3", data.VersionSets[0].Versions[3])
}

func TestCompatibleTargetsNotFound(t *testing.T) {
	data := mkInitialData(1)

	req := mkNewCompatReq("1.1", "1", false)
	err := UpdateVersionsGraph(data, req, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.NotFound{}, err)
}

func TestMakeExistingSetDefault(t *testing.T) {
	data := mkInitialData(4)

	req := mkExistingDefault("2")
	err := UpdateVersionsGraph(data, req, 0)

	assert.NoError(t, err)
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])
	assert.Equal(t, "1", data.VersionSets[1].Versions[0])
	assert.Equal(t, "3", data.VersionSets[2].Versions[0])
	assert.Equal(t, "2", data.VersionSets[3].Versions[0])

	// Add a compatible version to a set and then make that set the default via the compatible version
	req = mkNewCompatReq("1.1", "1", true)

	err = UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)
	assert.Equal(t, "0", data.VersionSets[0].Versions[0])
	assert.Equal(t, "3", data.VersionSets[1].Versions[0])
	assert.Equal(t, "2", data.VersionSets[2].Versions[0])
	assert.Equal(t, "1", data.VersionSets[3].Versions[0])
}

func TestSayVersionIsCompatWithDifferentSetThanItsAlreadyCompatWithNotAllowed(t *testing.T) {
	data := mkInitialData(3)

	req := mkNewCompatReq("0.1", "0", false)
	err := UpdateVersionsGraph(data, req, 0)
	assert.NoError(t, err)

	req = mkNewCompatReq("0.1", "1", false)
	err = UpdateVersionsGraph(data, req, 0)
	assert.Error(t, err)
	assert.IsType(t, &serviceerror.InvalidArgument{}, err)
}

func TestLimitsMaxSize(t *testing.T) {
	data := &persistencepb.VersioningData{}
	maxSize := 10

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("%d", i)
		req := mkNewDefReq(id)
		err := UpdateVersionsGraph(data, req, maxSize)
		assert.NoError(t, err)
	}

	for i := 0; i < len(data.VersionSets); i++ {
		assert.Equal(t, fmt.Sprintf("%d", i+10), data.VersionSets[i].Versions[0])
	}
}
