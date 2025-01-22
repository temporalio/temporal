// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package tasktoken

import (
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/utf8validator"
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
	if err := utf8validator.Validate(taskToken, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	return taskToken.Marshal()
}

func (s *Serializer) Deserialize(data []byte) (*tokenspb.Task, error) {
	taskToken := &tokenspb.Task{}
	err := taskToken.Unmarshal(data)
	if err == nil {
		err = utf8validator.Validate(taskToken, utf8validator.SourceRPCRequest)
	}
	return taskToken, err
}

func (s *Serializer) SerializeQueryTaskToken(taskToken *tokenspb.QueryTask) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	if err := utf8validator.Validate(taskToken, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	return taskToken.Marshal()
}

func (s *Serializer) DeserializeQueryTaskToken(data []byte) (*tokenspb.QueryTask, error) {
	taskToken := tokenspb.QueryTask{}
	err := taskToken.Unmarshal(data)
	if err == nil {
		err = utf8validator.Validate(&taskToken, utf8validator.SourceRPCRequest)
	}
	return &taskToken, err
}

func (s *Serializer) SerializeNexusTaskToken(taskToken *tokenspb.NexusTask) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	if err := utf8validator.Validate(taskToken, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	return taskToken.Marshal()
}

func (s *Serializer) DeserializeNexusTaskToken(data []byte) (*tokenspb.NexusTask, error) {
	taskToken := tokenspb.NexusTask{}
	err := taskToken.Unmarshal(data)
	if err == nil {
		err = utf8validator.Validate(&taskToken, utf8validator.SourceRPCRequest)
	}
	return &taskToken, err
}
