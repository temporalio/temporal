package testcore

import (
	"fmt"
	"regexp"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/versionhistory"
)

// TODO (alex): move this to functional_test_base.go as methods.

func RandomizeStr(id string) string {
	return fmt.Sprintf("%v-%v", id, uuid.New())
}

func DecodeString(t require.TestingT, pls *commonpb.Payloads) string {
	if th, ok := t.(interface{ Helper() }); ok {
		th.Helper()
	}
	var str string
	err := payloads.Decode(pls, &str)
	require.NoError(t, err)
	return str
}

func EventBatchesToVersionHistory(
	versionHistory *historyspb.VersionHistory,
	eventBatches []*historypb.History,
) (*historyspb.VersionHistory, error) {

	// TODO temporary code to generate version history
	//  we should generate version as part of modeled based testing
	if versionHistory == nil {
		versionHistory = versionhistory.NewVersionHistory(nil, nil)
	}
	for _, batch := range eventBatches {
		for _, event := range batch.Events {
			err := versionhistory.AddOrUpdateVersionHistoryItem(versionHistory,
				versionhistory.NewVersionHistoryItem(
					event.GetEventId(),
					event.GetVersion(),
				))
			if err != nil {
				return nil, err
			}
		}
	}

	return versionHistory, nil
}

func RandomizedNexusEndpoint(name string) string {
	re := regexp.MustCompile("[/_]")
	safeName := re.ReplaceAllString(name, "-")
	return fmt.Sprintf("%v-%v", safeName, uuid.New())
}
