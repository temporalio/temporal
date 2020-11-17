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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/service/config"
)

const (
	defaultPermissionsClaimName = "permissions"
)

// Default claim mapper that gives system level admin permission to everybody
type defaultClaimMapper struct {
	keyProvider          TokenKeyProvider
	logger               log.Logger
	permissionsClaimName string
}

func NewDefaultClaimMapper(provider TokenKeyProvider, cfg *config.Config) ClaimMapper {
	claimName := cfg.Global.Authorization.PermissionsClaimName
	if claimName == "" {
		claimName = defaultPermissionsClaimName
	}
	logger := loggerimpl.NewLogger(cfg.Log.NewZapLogger())
	return &defaultClaimMapper{keyProvider: provider, logger: logger, permissionsClaimName: claimName}
}

var _ ClaimMapper = (*defaultClaimMapper)(nil)

func (a *defaultClaimMapper) GetClaims(authInfo *AuthInfo) (*Claims, error) {

	claims := Claims{}

	if authInfo.authToken != "" {
		parts := strings.Split(authInfo.authToken, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("unexpected authorization token format")
		}
		if !strings.EqualFold(parts[0], "bearer") {
			return nil, fmt.Errorf("unexpected name in authorization token: %s", parts[0])
		}
		jwtClaims, err := parseJWT(parts[1], a.keyProvider)
		if err != nil {
			return nil, err
		}
		subject, ok := jwtClaims["sub"].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type of \"sub\" claim")
		}
		claims.subject = subject
		permissions, ok := jwtClaims[a.permissionsClaimName].([]interface{})
		if ok {
			for _, permission := range permissions {
				p, ok := permission.(string)
				if !ok {
					a.logger.Warn(fmt.Sprintf("ignoring permission that is not a string: %v", permission))
					continue
				}
				parts := strings.Split(p, ":")
				if len(parts) != 2 {
					a.logger.Warn(fmt.Sprintf("ignoring permission in unexpected format: %v", permission))
					continue
				}
				namespace := strings.ToLower(parts[0])
				if strings.EqualFold(namespace, "system") {
					claims.system |= permissionToRole(parts[1])
				} else {
					if claims.namespaces == nil {
						claims.namespaces = make(map[string]Role)
					}
					role := claims.namespaces[namespace]
					role |= permissionToRole(parts[1])
					claims.namespaces[namespace] = role
				}
			}
		}
	}
	return &claims, nil
}

func parseJWT(tokenString string, keyProvider TokenKeyProvider) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {

		kid, ok := token.Header["kid"].(string)
		if !ok {
			return nil, fmt.Errorf("malformed token - no \"kid\" header")
		}
		alg := token.Header["alg"].(string)
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); ok {
			return keyProvider.hmacKey(alg, kid)
		} else if _, ok := token.Method.(*jwt.SigningMethodRSA); ok {
			return keyProvider.rsaKey(alg, kid)
		} else if _, ok := token.Method.(*jwt.SigningMethodECDSA); ok {
			return keyProvider.ecdsaKey(alg, kid)
		}
		return nil, fmt.Errorf("unexpected signing method: %v for algorithm: %v", token.Method, token.Header["alg"])
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
	switch strings.ToLower(permission) {
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
