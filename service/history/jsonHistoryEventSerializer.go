package history

import (
	"encoding/json"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	jsonHistoryEventSerializer struct {
	}
)

func newJSONHistoryEventSerializer() historyEventSerializer {
	return &jsonHistoryEventSerializer{}
}

func (j *jsonHistoryEventSerializer) Serialize(event *workflow.HistoryEvent) ([]byte, error) {
	data, err := json.Marshal(event)

	return data, err
}

func (j *jsonHistoryEventSerializer) Deserialize(data []byte) (*workflow.HistoryEvent, error) {
	var history *workflow.HistoryEvent
	err := json.Unmarshal(data, &history)

	return history, err
}
