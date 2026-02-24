package sql

import (
	"encoding/json"
	"time"
)

type (
	pageTokenLegacy struct {
		CloseTime time.Time
		StartTime time.Time
		RunID     string
	}
)

func deserializePageTokenLegacy(data []byte) (*pageTokenLegacy, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var token *pageTokenLegacy
	err := json.Unmarshal(data, &token)
	return token, err
}

func serializePageTokenLegacy(token *pageTokenLegacy) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}
