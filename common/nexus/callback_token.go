package nexus

import (
	"encoding/base64"
	"encoding/json"

	tokenspb "go.temporal.io/server/api/token/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// Currently supported token version.
	TokenVersion = 1
	// Header key for the callback token in StartOperation requests.
	CallbackTokenHeader = "Temporal-Callback-Token"
)

// CallbackToken contains an encoded NexusOperationCompletion message.
type CallbackToken struct {
	// Token version - currently only [TokenVersion] is supported.
	Version int `json:"v"`
	// Encoded [tokenspb.NexusOperationCompletion].
	Data string `json:"d"`
	// More fields and encryption support will come later.
}

type CallbackTokenGenerator struct {
	// In the future this will contain more fields such as encryption keys.
}

func NewCallbackTokenGenerator() *CallbackTokenGenerator {
	return &CallbackTokenGenerator{}
}

func (g *CallbackTokenGenerator) Tokenize(completion *tokenspb.NexusOperationCompletion) (string, error) {
	b, err := proto.Marshal(completion)
	if err != nil {
		return "", err
	}
	token := CallbackToken{
		Version: TokenVersion,
		Data:    base64.URLEncoding.EncodeToString(b),
	}
	b, err = json.Marshal(token)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecodeCompletion decodes a callback token unwrapping the contained NexusOperationCompletion proto struct.
func (g *CallbackTokenGenerator) DecodeCompletion(token *CallbackToken) (*tokenspb.NexusOperationCompletion, error) {
	plaintext, err := base64.URLEncoding.DecodeString(token.Data)
	if err != nil {
		return nil, err
	}

	completion := &tokenspb.NexusOperationCompletion{}
	return completion, proto.Unmarshal(plaintext, completion)
}

// DecodeCallbackToken unmarshals the given token applying minimal data verification.
func DecodeCallbackToken(encoded string) (token *CallbackToken, err error) {
	err = json.Unmarshal([]byte(encoded), &token)
	if err != nil {
		return nil, err
	}
	if token.Version != TokenVersion {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported token version: %d", token.Version)
	}
	return
}
