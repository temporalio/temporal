package workflow

import "encoding/json"

type (
	jsonTaskTokenSerializer struct{}
)

func newJSONTaskTokenSerializer() taskTokenSerializer {
	return &jsonTaskTokenSerializer{}
}

func (j *jsonTaskTokenSerializer) Serialize(token *taskToken) ([]byte, error) {
	data, err := json.Marshal(token)

	return data, err
}

func (j *jsonTaskTokenSerializer) Deserialize(data []byte) (*taskToken, error) {
	var token taskToken
	err := json.Unmarshal(data, &token)

	return &token, err
}
