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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination claim_mapper_mock.go -self_package go.temporal.io/server/common/authorization

package authorization

import (
	"crypto/x509/pkix"

	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common/service/config"
)

// Authentication information from subject's JWT token or/and mTLS certificate
type AuthInfo struct {
	AuthToken     string
	TlsSubject    *pkix.Name
	TLSConnection *credentials.TLSInfo
}

// Converts authorization info of a subject into Temporal claims (permissions) for authorization
type ClaimMapper interface {
	GetClaims(authInfo *AuthInfo) (*Claims, error)
}

// No-op claim mapper that gives system level admin permission to everybody
type noopClaimMapper struct{}

var _ ClaimMapper = (*noopClaimMapper)(nil)

func NewNoopClaimMapper(_ *config.Config) ClaimMapper {
	return &noopClaimMapper{}
}

func (*noopClaimMapper) GetClaims(_ *AuthInfo) (*Claims, error) {
	return &Claims{system: RoleAdmin}, nil
}
