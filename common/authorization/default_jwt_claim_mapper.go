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
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

const (
	defaultPermissionsClaimName = "permissions"
	authorizationBearer         = "bearer"
	headerSubject               = "sub"
	permissionScopeSystem       = "system"
	permissionRead              = "read"
	permissionWrite             = "write"
	permissionWorker            = "worker"
	permissionAdmin             = "admin"
)

// Default claim mapper that gives system level admin permission to everybody
type defaultJWTClaimMapper struct {
	keyProvider          TokenKeyProvider
	logger               log.Logger
	permissionsClaimName string
}

func NewDefaultJWTClaimMapper(provider TokenKeyProvider, cfg *config.Authorization, logger log.Logger) ClaimMapper {
	claimName := cfg.PermissionsClaimName
	if claimName == "" {
		claimName = defaultPermissionsClaimName
	}
	return &defaultJWTClaimMapper{keyProvider: provider, logger: logger, permissionsClaimName: claimName}
}

var _ ClaimMapper = (*defaultJWTClaimMapper)(nil)

func (a *defaultJWTClaimMapper) GetClaims(authInfo *AuthInfo) (*Claims, error) {

	claims := Claims{}

	if authInfo.AuthToken == "" {
		return &claims, nil
	}

	parts := strings.Split(authInfo.AuthToken, " ")
	if len(parts) != 2 {
		return nil, serviceerror.NewPermissionDenied("unexpected authorization token format", "")
	}
	if !strings.EqualFold(parts[0], authorizationBearer) {
		return nil, serviceerror.NewPermissionDenied("unexpected name in authorization token", "")
	}
	jwtClaims, err := parseJWTWithAudience(parts[1], a.keyProvider, authInfo.Audience)
	if err != nil {
		return nil, err
	}
	subject, ok := jwtClaims[headerSubject].(string)
	if !ok {
		return nil, serviceerror.NewPermissionDenied("unexpected value type of \"sub\" claim", "")
	}
	claims.Subject = subject
	permissions, ok := jwtClaims[a.permissionsClaimName].([]interface{})
	if ok {
		err := a.extractPermissions(permissions, &claims)
		if err != nil {
			return nil, err
		}
	}
	return &claims, nil
}

func (a *defaultJWTClaimMapper) extractPermissions(permissions []interface{}, claims *Claims) error {
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
		if strings.EqualFold(namespace, permissionScopeSystem) {
			claims.System |= permissionToRole(parts[1])
		} else {
			if claims.Namespaces == nil {
				claims.Namespaces = make(map[string]Role)
			}
			role := claims.Namespaces[namespace]
			role |= permissionToRole(parts[1])
			claims.Namespaces[namespace] = role
		}
	}
	return nil
}

func parseJWT(tokenString string, keyProvider TokenKeyProvider) (jwt.MapClaims, error) {
	return parseJWTWithAudience(tokenString, keyProvider, "")
}

func parseJWTWithAudience(tokenString string, keyProvider TokenKeyProvider, audience string) (jwt.MapClaims, error) {

	parser := jwt.NewParser(jwt.WithValidMethods(keyProvider.SupportedMethods()))

	var keyFunc jwt.Keyfunc
	if provider, _ := keyProvider.(RawTokenKeyProvider); provider != nil {
		keyFunc = func(token *jwt.Token) (interface{}, error) {
			// reserve context
			// impl may introduce network request to get public key
			return provider.GetKey(context.Background(), token)
		}
	} else {
		keyFunc = func(token *jwt.Token) (interface{}, error) {
			kid, ok := token.Header["kid"].(string)
			if !ok {
				return nil, fmt.Errorf("malformed token - no \"kid\" header")
			}
			alg := token.Header["alg"].(string)
			switch token.Method.(type) {
			case *jwt.SigningMethodHMAC:
				return keyProvider.HmacKey(alg, kid)
			case *jwt.SigningMethodRSA:
				return keyProvider.RsaKey(alg, kid)
			case *jwt.SigningMethodECDSA:
				return keyProvider.EcdsaKey(alg, kid)
			default:
				return nil, serviceerror.NewPermissionDenied(
					fmt.Sprintf("unexpected signing method: %v for algorithm: %v", token.Method, token.Header["alg"]), "")
			}
		}
	}

	token, err := parser.Parse(tokenString, keyFunc)

	if err != nil {
		return nil, err
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, serviceerror.NewPermissionDenied("invalid token with no claims", "")
	}
	if err := claims.Valid(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(audience) != "" && !claims.VerifyAudience(audience, true) {
		return nil, serviceerror.NewPermissionDenied("audience mismatch", "")
	}
	return claims, nil
}

func permissionToRole(permission string) Role {
	switch strings.ToLower(permission) {
	case permissionRead:
		return RoleReader
	case permissionWrite:
		return RoleWriter
	case permissionAdmin:
		return RoleAdmin
	case permissionWorker:
		return RoleWorker
	}
	return RoleUndefined
}
