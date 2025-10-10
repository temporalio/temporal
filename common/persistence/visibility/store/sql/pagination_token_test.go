package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSerializePageToken(t *testing.T) {
	s := assert.New(t)

	token := pageToken{
		CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
		StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
		RunID:     "test-run-id",
	}
	data, err := serializePageToken(&token)
	s.NoError(err)
	s.JSONEq(
		`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id"}`,
		string(data),
	)
}

func TestDeserializePageToken(t *testing.T) {
	s := assert.New(t)

	token, err := deserializePageToken(nil)
	s.NoError(err)
	s.Nil(token)

	token, err = deserializePageToken([]byte{})
	s.NoError(err)
	s.Nil(token)

	token, err = deserializePageToken(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id"}`),
	)
	s.NoError(err)
	s.NotNil(token)
	s.Equal(
		pageToken{
			CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
			StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
			RunID:     "test-run-id",
		},
		*token,
	)
}
