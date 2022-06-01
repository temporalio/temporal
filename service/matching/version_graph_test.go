package matching

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/workflowservice/v1"
	"testing"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
)

func mkVerIdNode(id string) *workflowpb.VersionIdNode {
	return &workflowpb.VersionIdNode{
		Version: mkVerId(id),
	}
}

func mkVerId(id string) *workflowpb.VersionId {
	return &workflowpb.VersionId{
		Version: &workflowpb.VersionId_WorkerBuildId{WorkerBuildId: id},
	}
}

func TestNewDefaultGraphUpdate(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1.PreviousIncompatible = n0
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:     mkVerId("2"),
		BecomeDefault: true,
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(req.VersionId))
	assert.Equal(t, "2", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Equal(t, n1, data.CurrentDefaults[0].PreviousIncompatible)
	assert.Equal(t, "1", data.CurrentDefaults[0].PreviousIncompatible.Version.GetWorkerBuildId())
	assert.Equal(t, n0, data.CurrentDefaults[0].PreviousIncompatible.PreviousIncompatible)
	assert.Equal(t, "0", data.CurrentDefaults[0].PreviousIncompatible.PreviousIncompatible.Version.GetWorkerBuildId())

	asResp := ToBuildIdOrderingResponse(data)
	assert.Equal(t, 1, len(asResp.GetCurrentDefaults()))
	assert.Equal(t, 0, len(asResp.GetCompatibleLeaves()))
}

func TestNewDefaultGraphUpdateOfEmptyGraph(t *testing.T) {
	data := &persistencepb.VersioningData{}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:     mkVerId("1"),
		BecomeDefault: true,
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(req.VersionId))
	assert.Equal(t, "1", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Nil(t, data.CurrentDefaults[0].GetPreviousIncompatible())
	assert.Nil(t, data.CurrentDefaults[0].GetPreviousCompatible())
}

func TestNewDefaultGraphUpdateCompatWithCurDefault(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1.PreviousIncompatible = n0
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("2"),
		PreviousCompatible: mkVerId("1"),
		BecomeDefault:      true,
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(req.VersionId))
	assert.Equal(t, "2", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Equal(t, n1, data.CurrentDefaults[0].PreviousCompatible)
	assert.Equal(t, "1", data.CurrentDefaults[0].PreviousCompatible.Version.GetWorkerBuildId())
	assert.Equal(t, n0, data.CurrentDefaults[0].PreviousIncompatible)
	assert.Equal(t, "0", data.CurrentDefaults[0].PreviousIncompatible.Version.GetWorkerBuildId())
}

func TestNewCompatibleWithNodeDeepInIncompatChain(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1.PreviousIncompatible = n0
	n2 := mkVerIdNode("2")
	n2.PreviousIncompatible = n1
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n2},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("0.1"),
		PreviousCompatible: mkVerId("0"),
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.Equal(t, "2", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.True(t, data.CompatibleLeaves[0].Version.Equal(req.VersionId))
	assert.Equal(t, "0.1", data.CompatibleLeaves[0].Version.GetWorkerBuildId())
	assert.Equal(t, "0", data.CompatibleLeaves[0].PreviousCompatible.Version.GetWorkerBuildId())

	asResp := ToBuildIdOrderingResponse(data)
	assert.Equal(t, 1, len(asResp.GetCurrentDefaults()))
	assert.Equal(t, 1, len(asResp.GetCompatibleLeaves()))
	assert.Equal(t, "0.1", asResp.CompatibleLeaves[0].Version.GetWorkerBuildId())
}

func TestNewCompatibleWithNonDefaultGraphUpdate(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1.PreviousIncompatible = n0
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("0.1"),
		PreviousCompatible: mkVerId("0"),
	}
	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(n1.Version))
	assert.Equal(t, "1", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Equal(t, 1, len(data.CompatibleLeaves))
	assert.True(t, data.CompatibleLeaves[0].Version.Equal(req.VersionId))
	assert.Equal(t, "0.1", data.CompatibleLeaves[0].Version.GetWorkerBuildId())

	req = &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("0.2"),
		PreviousCompatible: mkVerId("0.1"),
	}
	err = UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(n1.Version))
	assert.Equal(t, "1", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Equal(t, 1, len(data.CompatibleLeaves))
	assert.True(t, data.CompatibleLeaves[0].Version.Equal(req.VersionId))
	assert.Equal(t, "0.2", data.CompatibleLeaves[0].Version.GetWorkerBuildId())

	// Ensure setting a compatible version which targets a non-leaf compat version ends up without a branch
	req = &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("0.3"),
		PreviousCompatible: mkVerId("0.1"),
	}
	err = UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(n1.Version))
	assert.Equal(t, "1", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Equal(t, 1, len(data.CompatibleLeaves))
	assert.True(t, data.CompatibleLeaves[0].Version.Equal(req.VersionId))
	assert.Equal(t, "0.3", data.CompatibleLeaves[0].Version.GetWorkerBuildId())
	assert.Equal(t, "0.2", data.CompatibleLeaves[0].PreviousCompatible.Version.GetWorkerBuildId())
}

func TestAddingNewNodeCompatWithPreviousWhenNoDefaultNotAllowed(t *testing.T) {
	data := &persistencepb.VersioningData{}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("0.1"),
		PreviousCompatible: mkVerId("0"),
		BecomeDefault:      true,
	}
	err := UpdateVersionsGraph(data, req)
	assert.Error(t, err)
	assert.IsType(t, &invalidVersionUpdateError{}, err)
}

func TestAddingNewNodeWithNoLinksNotAllowed(t *testing.T) {
	data := &persistencepb.VersioningData{}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId: mkVerId("0.1"),
	}
	err := UpdateVersionsGraph(data, req)
	assert.Error(t, err)
	assert.IsType(t, &invalidVersionUpdateError{}, err)
}

func TestUnsetCurrentDefault(t *testing.T) {
	n1 := mkVerIdNode("1")
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId: mkVerId("1"),
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(data.CurrentDefaults))
}

func TestUnsetCurrentDefaultPreviousIncompatBecomesDefault(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1.PreviousIncompatible = n0
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId: mkVerId("1"),
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(n0.Version))
	assert.Equal(t, "0", data.CurrentDefaults[0].Version.GetWorkerBuildId())
}

func TestUnsetCurrentDefaultPreviousCompatBecomesDefault(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1d1 := mkVerIdNode("1.1")
	n1.PreviousIncompatible = n0
	n1d1.PreviousCompatible = n1
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n1d1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId: mkVerId("1.1"),
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(n1.Version))
	assert.Equal(t, "1", data.CurrentDefaults[0].Version.GetWorkerBuildId())
}

func TestDropCompatibleLeaf(t *testing.T) {
	n0 := mkVerIdNode("0")
	n1 := mkVerIdNode("1")
	n1d1 := mkVerIdNode("1.1")
	n1.PreviousIncompatible = n0
	n1d1.PreviousCompatible = n1
	data := &persistencepb.VersioningData{
		CurrentDefaults:  []*workflowpb.VersionIdNode{n1},
		CompatibleLeaves: []*workflowpb.VersionIdNode{n1d1},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId: mkVerId("1.1"),
	}

	err := UpdateVersionsGraph(data, req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(data.CurrentDefaults))
	assert.True(t, data.CurrentDefaults[0].Version.Equal(n1.Version))
	assert.Equal(t, "1", data.CurrentDefaults[0].Version.GetWorkerBuildId())
	assert.Equal(t, 1, len(data.CompatibleLeaves))
	assert.Equal(t, "1", data.CompatibleLeaves[0].Version.GetWorkerBuildId())
}

func TestCompatibleTargetsNotFound(t *testing.T) {
	n0 := mkVerIdNode("0")
	data := &persistencepb.VersioningData{
		CurrentDefaults: []*workflowpb.VersionIdNode{n0},
	}

	req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		VersionId:          mkVerId("1.1"),
		PreviousCompatible: mkVerId("1"),
	}

	err := UpdateVersionsGraph(data, req)
	assert.Error(t, err)
	assert.IsType(t, &versionNotFoundError{}, err)
}

func FuzzVersionGraphEnsureNoSameTypeDefaults(f *testing.F) {
	f.Fuzz(func(t *testing.T, numUpdates, willPickCompatMod, compatModTarget uint8) {
		addedNodes := make([]*workflowpb.VersionId, 0, numUpdates)
		data := &persistencepb.VersioningData{}

		for i := uint8(0); i < numUpdates; i++ {
			id := mkVerId(fmt.Sprintf("%d", i))
			req := &workflowservice.UpdateWorkerBuildIdOrderingRequest{
				VersionId:     id,
				BecomeDefault: true,
			}
			if willPickCompatMod > 0 && compatModTarget > 0 &&
				numUpdates%willPickCompatMod == 0 &&
				uint8(len(addedNodes)) > numUpdates%compatModTarget {
				compatTarget := addedNodes[numUpdates%compatModTarget]
				req.PreviousCompatible = compatTarget
			}
			addedNodes = append(addedNodes, id)
			err := UpdateVersionsGraph(data, req)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(data.CurrentDefaults))
			assert.NotNil(t, ToBuildIdOrderingResponse(data))
		}
	})
}
