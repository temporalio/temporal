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

package api

import (
	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
)

func GetActiveNamespace(
	shard shard.Context,
	namespaceUUID namespace.ID,
) (*namespace.Namespace, error) {

	err := ValidateNamespaceUUID(namespaceUUID)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespaceUUID)
	if err != nil {
		return nil, err
	}
	if !namespaceEntry.ActiveInCluster(shard.GetClusterMetadata().GetCurrentClusterName()) {
		return nil, serviceerror.NewNamespaceNotActive(
			namespaceEntry.Name().String(),
			shard.GetClusterMetadata().GetCurrentClusterName(),
			namespaceEntry.ActiveClusterName())
	}
	return namespaceEntry, nil
}

func ValidateNamespaceUUID(
	namespaceUUID namespace.ID,
) error {
	if namespaceUUID == "" {
		return serviceerror.NewInvalidArgument("Missing namespace UUID.")
	} else if uuid.Parse(namespaceUUID.String()) == nil {
		return serviceerror.NewInvalidArgument("Invalid namespace UUID.")
	}
	return nil
}
