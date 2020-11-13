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
	"fmt"
	"strings"

	"github.com/dgrijalva/jwt-go"
)

// Default claim mapper that gives system level admin permission to everybody
type defaultClaimMapper struct {
	keyProvider TokenKeyProvider
}

func NewDefaultClaimMapper(provider TokenKeyProvider) ClaimMapper {
	return &defaultClaimMapper{keyProvider: provider}
}

var _ ClaimMapper = (*defaultClaimMapper)(nil)

func (a *defaultClaimMapper) GetClaims(authInfo AuthInfo) (*Claims, error) {

	claims := Claims{}

	if authInfo.authToken != "" {
		parts := strings.Split(authInfo.authToken, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("unexpected authorization token format")
		}
		name := strings.ToLower(parts[0])
		if name != "bearer" {
			return nil, fmt.Errorf("unexpected name in authorization token: %s", name)
		}
		jwtClaims, err := parseJWT(parts[1], a.keyProvider)
		if err != nil {
			return nil, err
		}
		claims.subject = jwtClaims["sub"].(string)
		permissions := jwtClaims["permissions"].([]interface{})
		for _, permission := range permissions {
			parts := strings.Split(permission.(string), ":")
			if len(parts) == 2 {
				namespace := parts[0]
				if namespace == "system" {
					claims.system |= permissionToRole(parts[1])
				} else {
					if claims.namespaces == nil {
						claims.namespaces = make(map[string]Role)
					}
					role := claims.namespaces[namespace]
					role |= permissionToRole(parts[0])
					claims.namespaces[namespace] = role
				}
			}
		}
	}
	return &claims, nil
}

func parseJWT(tokenString string, keyProvider TokenKeyProvider) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {

		kid := token.Header["kid"].(string)
		alg := token.Header["alg"].(string)
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); ok {
			return keyProvider.hmacKey(alg, kid)
		} else if _, ok := token.Method.(*jwt.SigningMethodRSA); ok {
			return keyProvider.rsaKey(alg, kid)
		} else if _, ok := token.Method.(*jwt.SigningMethodECDSA); ok {
			return keyProvider.ecdsaKey(alg, kid)
		}
		return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		return claims, nil
	}
	return nil, err
}

func permissionToRole(permission string) Role {
	switch permission {
	case "read":
		return RoleReader
	case "write":
		return RoleWriter
	case "admin":
		return RoleAdmin
	case "worker":
		return RoleWorker
	}
	return RoleUndefined
}
