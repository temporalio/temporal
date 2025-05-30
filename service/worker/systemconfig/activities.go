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

package systemconfig

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/persistence"
)

type (
	UpdateFailoverVersionIncrementInput struct {
		CurrentFVI int64
		NewFVI     int64
	}

	UpdateFailoverVersionIncrementOutput struct {
	}

	activities struct {
		currentClusterName     string
		clusterMetadataManager persistence.ClusterMetadataManager
	}
)

func (a *activities) UpdateFVI(
	ctx context.Context,
	input UpdateFailoverVersionIncrementInput,
) (UpdateFailoverVersionIncrementOutput, error) {
	request := &persistence.GetClusterMetadataRequest{
		ClusterName: a.currentClusterName,
	}
	if input.NewFVI <= 0 {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New("invalid failover version increment")
	}
	resp, err := a.clusterMetadataManager.GetClusterMetadata(ctx, request)
	if err != nil {
		return UpdateFailoverVersionIncrementOutput{}, err
	}
	metadata := resp.ClusterMetadata
	if metadata.FailoverVersionIncrement != input.CurrentFVI {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New(fmt.Sprintf("failover version increment %v does not match input version %v", metadata.FailoverVersionIncrement, input.CurrentFVI))
	}
	if metadata.InitialFailoverVersion == input.NewFVI {
		return UpdateFailoverVersionIncrementOutput{}, nil
	}
	if metadata.InitialFailoverVersion > input.NewFVI {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New(fmt.Sprintf("failover version increment %v is less than initial failover version %v", input.NewFVI, metadata.InitialFailoverVersion))
	}
	if !metadata.IsGlobalNamespaceEnabled {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New("please update failover version increment from application yaml file")
	}

	metadata.FailoverVersionIncrement = input.NewFVI
	applied, err := a.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: metadata,
		Version:         resp.Version,
	})
	if err != nil {
		return UpdateFailoverVersionIncrementOutput{}, err
	}
	if !applied {
		return UpdateFailoverVersionIncrementOutput{}, errors.New("new failover version increment did not apply")
	}
	return UpdateFailoverVersionIncrementOutput{}, nil
}
