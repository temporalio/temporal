package workflow

import (
	"encoding/json"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
)

type (
	jsonHistorySerializer struct {
	}
)

func newJSONHistorySerializer() historySerializer {
	return &jsonHistorySerializer{}
}

func (j *jsonHistorySerializer) Serialize(history []*workflow.HistoryEvent) ([]byte, error) {
	data, err := json.Marshal(history)

	return data, err
}

func (j *jsonHistorySerializer) Deserialize(data []byte) ([]*workflow.HistoryEvent,
	error) {
	var history []*workflow.HistoryEvent
	err := json.Unmarshal(data, &history)

	return history, err
}
