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

package authorization

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"

	"github.com/golang-jwt/jwt/v4"
)

// @@@SNIPSTART temporal-common-authorization-tokenkeyprovider-interface
// Provides keys for validating JWT tokens
type TokenKeyProvider interface {
	EcdsaKey(alg string, kid string) (*ecdsa.PublicKey, error)
	HmacKey(alg string, kid string) ([]byte, error)
	RsaKey(alg string, kid string) (*rsa.PublicKey, error)
	SupportedMethods() []string
	Close()
}

// RawTokenKeyProvider is a TokenKeyProvider that provides keys for validating JWT tokens
type RawTokenKeyProvider interface {
	GetKey(ctx context.Context, token *jwt.Token) (interface{}, error)
	SupportedMethods() []string
	Close()
}

// @@@SNIPEND
