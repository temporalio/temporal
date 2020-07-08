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

package common

import (
	tokenspb "go.temporal.io/server/api/token/v1"
)

type (
	protoTaskTokenSerializer struct{}
)

// NewProtoTaskTokenSerializer creates a new instance of TaskTokenSerializer
func NewProtoTaskTokenSerializer() TaskTokenSerializer {
	return &protoTaskTokenSerializer{}
}

func (j *protoTaskTokenSerializer) Serialize(taskToken *tokenspb.Task) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (j *protoTaskTokenSerializer) Deserialize(data []byte) (*tokenspb.Task, error) {
	taskToken := &tokenspb.Task{}
	err := taskToken.Unmarshal(data)
	return taskToken, err
}

func (j *protoTaskTokenSerializer) SerializeQueryTaskToken(taskToken *tokenspb.QueryTask) ([]byte, error) {
	if taskToken == nil {
		return nil, nil
	}
	return taskToken.Marshal()
}

func (j *protoTaskTokenSerializer) DeserializeQueryTaskToken(data []byte) (*tokenspb.QueryTask, error) {
	taskToken := tokenspb.QueryTask{}
	err := taskToken.Unmarshal(data)
	return &taskToken, err
}
