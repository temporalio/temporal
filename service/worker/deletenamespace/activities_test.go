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

package deletenamespace

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

func Test_GenerateDeletedNamespaceNameActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)

	a := &activities{
		metadataManager: metadataManager,
		metricsClient:   metrics.NoopClient,
		logger:          log.NewNoopLogger(),
	}

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-names",
	}).Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-names"))
	deletedName, err := a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-names"), deletedName)

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-names",
	}).Return(nil, nil)
	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-namesp",
	}).Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-namesp"))
	deletedName, err = a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-namesp"), deletedName)

	ctrl.Finish()
}
