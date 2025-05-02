package persistence

import "encoding/json"

type (
	jsonHistoryTokenSerializer struct{}
	// historyPagingToken is used to serialize/deserialize pagination token for ReadHistoryBranchRequest
	historyPagingToken struct {
		LastEventID int64
		// the pagination token passing to persistence
		StoreToken []byte
		// recording which branchRange it is reading
		CurrentRangeIndex int
		FinalRangeIndex   int
		// LastNodeID is the last known node ID attached to a history node
		LastNodeID int64
		// LastTransactionID is the last known transaction ID attached to a history node
		LastTransactionID int64
	}
)

const notStartedIndex = -1

// newJSONHistoryTokenSerializer creates a new instance of TaskTokenSerializer
func newJSONHistoryTokenSerializer() *jsonHistoryTokenSerializer {
	return &jsonHistoryTokenSerializer{}
}

func (t *historyPagingToken) SetRangeIndexes(
	current int,
	final int,
) {

	t.CurrentRangeIndex = current
	t.FinalRangeIndex = final
}

func (j *jsonHistoryTokenSerializer) Serialize(
	token *historyPagingToken,
) ([]byte, error) {

	data, err := json.Marshal(token)
	return data, err
}

func (j *jsonHistoryTokenSerializer) Deserialize(
	data []byte,
	defaultLastEventID int64,
	defaultLastNodeID int64,
	defaultLastTransactionID int64,
) (*historyPagingToken, error) {

	if len(data) == 0 {
		token := historyPagingToken{
			LastEventID:       defaultLastEventID,
			CurrentRangeIndex: notStartedIndex,
			LastNodeID:        defaultLastNodeID,
			LastTransactionID: defaultLastTransactionID,
		}
		return &token, nil
	}

	token := historyPagingToken{}
	err := json.Unmarshal(data, &token)
	return &token, err
}
