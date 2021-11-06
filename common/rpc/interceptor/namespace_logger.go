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

package interceptor

import (
	"context"
	"crypto/md5"
	"fmt"

	"google.golang.org/grpc"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
)

type (
	NamespaceLogInterceptor struct {
		namespaceRegistry namespace.Registry
		logger            log.Logger
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceLogInterceptor)(nil).Intercept

func NewNamespaceLogInterceptor(namespaceRegistry namespace.Registry, logger log.Logger) *NamespaceLogInterceptor {

	return &NamespaceLogInterceptor{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
	}
}

func (nli *NamespaceLogInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	if nli.logger != nil {
		_, methodName := splitMethodName(info.FullMethod)
		namespace := GetNamespace(nli.namespaceRegistry, req)
		tlsInfo := authorization.TLSInfoFormContext(ctx)
		var serverName string
		var certThumbprint string
		if tlsInfo != nil {
			serverName = tlsInfo.State.ServerName
			cert := authorization.PeerCert(tlsInfo)
			if cert != nil {
				certThumbprint = fmt.Sprintf("%x", md5.Sum(cert.Raw))
			}
		}
		nli.logger.Debug(
			"Frontend method invoked.",
			tag.WorkflowNamespace(namespace.String()),
			tag.Operation(methodName),
			tag.ServerName(serverName),
			tag.CertThumbprint(certThumbprint))
	}
	return handler(ctx, req)
}
