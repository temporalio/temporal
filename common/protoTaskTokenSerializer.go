package common

import (
	tokengenpb "github.com/temporalio/temporal/.gen/proto/token"
)

type (
	protoTaskTokenSerializer struct{}
)

// NewProtoTaskTokenSerializer creates a new instance of TaskTokenSerializer
func NewProtoTaskTokenSerializer() TaskTokenSerializer {
	return &protoTaskTokenSerializer{}
}

func (j *protoTaskTokenSerializer) Serialize(taskToken *tokengenpb.Task) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (j *protoTaskTokenSerializer) Deserialize(data []byte) (*tokengenpb.Task, error) {
	taskToken := &tokengenpb.Task{}
	err := taskToken.Unmarshal(data)
	return taskToken, err
}

func (j *protoTaskTokenSerializer) SerializeQueryTaskToken(taskToken *tokengenpb.QueryTask) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (j *protoTaskTokenSerializer) DeserializeQueryTaskToken(data []byte) (*tokengenpb.QueryTask, error) {
	taskToken := tokengenpb.QueryTask{}
	err := taskToken.Unmarshal(data)
	return &taskToken, err
}
