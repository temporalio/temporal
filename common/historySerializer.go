package common

import (
	"encoding/json"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	// HistorySerializer is used to serialize/deserialize history
	HistorySerializer interface {
		Serialize(history []*workflow.HistoryEvent) ([]byte, error)
		Deserialize(data []byte) ([]*workflow.HistoryEvent, error)
	}

	jsonHistorySerializer struct {
	}
)

// NewJSONHistorySerializer returns a JSON HistorySerializer
func NewJSONHistorySerializer() HistorySerializer {
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
