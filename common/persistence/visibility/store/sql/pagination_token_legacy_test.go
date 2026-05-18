package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSerializePageToken(t *testing.T) {
	s := assert.New(t)

	qt := time.Date(2023, 3, 21, 14, 0, 32, 0, time.UTC)
	token := pageTokenLegacy{
		CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
		StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
		RunID:     "test-run-id",
		QueryTime: &qt,
	}
	data, err := serializePageTokenLegacy(&token)
	s.NoError(err)
	s.Equal(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id","QueryTime":"2023-03-21T14:00:32Z"}`),
		data,
	)
}

func TestDeserializePageToken(t *testing.T) {
	s := assert.New(t)

	token, err := deserializePageTokenLegacy(nil)
	s.NoError(err)
	s.Nil(token)

	token, err = deserializePageTokenLegacy([]byte{})
	s.NoError(err)
	s.Nil(token)

	token, err = deserializePageTokenLegacy(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id","QueryTime":"2023-03-21T14:00:32Z"}`),
	)
	s.NoError(err)
	s.NotNil(token)
	qt2 := time.Date(2023, 3, 21, 14, 0, 32, 0, time.UTC)
	s.Equal(
		pageTokenLegacy{
			CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
			StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
			RunID:     "test-run-id",
			QueryTime: &qt2,
		},
		*token,
	)
}
