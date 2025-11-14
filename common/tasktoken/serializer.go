package tasktoken

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
)

type Serializer struct{}

// NewSerializer creates a new instance of Serializer
func NewSerializer() *Serializer {
	return &Serializer{}
}

func (s *Serializer) Serialize(taskToken *tokenspb.Task) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (s *Serializer) Deserialize(data []byte) (*tokenspb.Task, error) {
	taskToken := &tokenspb.Task{}
	err := taskToken.Unmarshal(data)
	return taskToken, err
}

func (s *Serializer) SerializeQueryTaskToken(taskToken *tokenspb.QueryTask) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (s *Serializer) DeserializeQueryTaskToken(data []byte) (*tokenspb.QueryTask, error) {
	taskToken := tokenspb.QueryTask{}
	err := taskToken.Unmarshal(data)
	return &taskToken, err
}

func (s *Serializer) SerializeNexusTaskToken(taskToken *tokenspb.NexusTask) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (s *Serializer) DeserializeNexusTaskToken(data []byte) (*tokenspb.NexusTask, error) {
	taskToken := tokenspb.NexusTask{}
	err := taskToken.Unmarshal(data)
	return &taskToken, err
}

func (s *Serializer) DeserializeChasmComponentRef(data []byte) (*persistencespb.ChasmComponentRef, error) {
	token := persistencespb.ChasmComponentRef{}
	err := token.Unmarshal(data)
	return &token, err
}
