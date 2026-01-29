package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSerializePageToken(t *testing.T) {
	s := assert.New(t)

	token := pageTokenLegacy{
		CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
		StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
		RunID:     "test-run-id",
	}
	data, err := serializePageTokenLegacy(&token)
	s.NoError(err)
	s.Equal(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id"}`),
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
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id"}`),
	)
	s.NoError(err)
	s.NotNil(token)
	s.Equal(
		pageTokenLegacy{
			CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
			StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
			RunID:     "test-run-id",
		},
		*token,
	)
}
