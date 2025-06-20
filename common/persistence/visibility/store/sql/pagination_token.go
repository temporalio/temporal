package sql

import (
	"encoding/json"
	"time"
)

type (
	pageToken struct {
		CloseTime time.Time
		StartTime time.Time
		RunID     string
	}
)

func deserializePageToken(data []byte) (*pageToken, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var token *pageToken
	err := json.Unmarshal(data, &token)
	return token, err
}

func serializePageToken(token *pageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}
