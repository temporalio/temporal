package sqlplugin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSerializeVisibilityPageToken(t *testing.T) {
	r := require.New(t)

	qt := time.Date(2023, 3, 21, 14, 0, 32, 0, time.UTC)
	token := VisibilityPageToken{
		CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
		StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
		RunID:     "test-run-id",
		QueryTime: &qt,
	}
	data, err := SerializeVisibilityPageToken(&token)
	r.NoError(err)
	r.JSONEq(
		`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id","QueryTime":"2023-03-21T14:00:32Z"}`,
		string(data),
	)
}

func TestDeserializeVisibilityPageToken(t *testing.T) {
	r := require.New(t)

	token, err := DeserializeVisibilityPageToken(nil)
	r.NoError(err)
	r.Nil(token)

	token, err = DeserializeVisibilityPageToken([]byte{})
	r.NoError(err)
	r.Nil(token)

	token, err = DeserializeVisibilityPageToken(
		[]byte(`{"CloseTime":"2023-03-21T14:20:32Z","StartTime":"2023-03-21T14:10:32Z","RunID":"test-run-id","QueryTime":"2023-03-21T14:00:32Z"}`),
	)
	r.NoError(err)
	r.NotNil(token)
	qt := time.Date(2023, 3, 21, 14, 0, 32, 0, time.UTC)
	r.Equal(
		VisibilityPageToken{
			CloseTime: time.Date(2023, 3, 21, 14, 20, 32, 0, time.UTC),
			StartTime: time.Date(2023, 3, 21, 14, 10, 32, 0, time.UTC),
			RunID:     "test-run-id",
			QueryTime: &qt,
		},
		*token,
	)
}
