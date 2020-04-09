package common

import (
	tokengenpb "github.com/temporalio/temporal/.gen/proto/token"
)

type (
	// TaskTokenSerializer serializes task tokens
	TaskTokenSerializer interface {
		Serialize(token *tokengenpb.Task) ([]byte, error)
		Deserialize(data []byte) (*tokengenpb.Task, error)
		SerializeQueryTaskToken(token *tokengenpb.QueryTask) ([]byte, error)
		DeserializeQueryTaskToken(data []byte) (*tokengenpb.QueryTask, error)
	}
)
