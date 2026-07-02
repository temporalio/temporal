package nexus

import (
	"encoding/base64"
	"encoding/json"

	"google.golang.org/protobuf/proto"
)

const (
	// Currently supported token version.
	OperationTokenVersion = 1
)

// OperationToken is the wire envelope for the token returned by an asynchronous Nexus operation
// handler. It mirrors the structure of [CallbackToken] so the token format can evolve (versioning,
// encryption) without breaking existing tokens. The inner payload is service-specific: each operation
// supplies its own proto message, so services can extend their token independently.
type OperationToken struct {
	// Token version - currently only [TokenVersion] is supported.
	Version int `json:"v"`
	// Base64-encoded service-specific token payload proto.
	Data string `json:"d"`
}

// GenerateOperationToken encodes the given service-specific token payload into the wire token string:
// the proto is base64-encoded and wrapped in a versioned JSON envelope.
func GenerateOperationToken(payload proto.Message) (string, error) {
	b, err := proto.Marshal(payload)
	if err != nil {
		return "", err
	}
	token := OperationToken{
		Version: OperationTokenVersion,
		Data:    base64.URLEncoding.EncodeToString(b),
	}
	b, err = json.Marshal(token)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
