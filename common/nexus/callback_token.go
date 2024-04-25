// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
