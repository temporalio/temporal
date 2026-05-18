package authorization

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	"github.com/jmespath/go-jmespath"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
)

const (
	defaultPermissionsClaimName = "permissions"
	authorizationBearer         = "bearer"
	headerSubject               = "sub"
	permissionScopeSystem       = primitives.SystemLocalNamespace
	permissionRead              = "read"
	permissionWrite             = "write"
	permissionWorker            = "worker"
	permissionAdmin             = "admin"
)

// Default claim mapper that gives system level admin permission to everybody
type defaultJWTClaimMapper struct {
	keyProvider           TokenKeyProvider
	logger                log.Logger
	permissionsClaimName  string
	permissionsClaimQuery *jmespath.JMESPath
	permissionsRegex      *regexp.Regexp
	matchNamespaceIndex   int
	matchRoleIndex        int
}

func NewDefaultJWTClaimMapper(provider TokenKeyProvider, cfg *config.Authorization, logger log.Logger) ClaimMapper {
	permissionsClaimName := cfg.PermissionsClaimName
	var permissionsClaimQuery *jmespath.JMESPath
	if cfg.PermissionsClaimQuery != "" {
		var err error
		permissionsClaimQuery, err = jmespath.Compile(cfg.PermissionsClaimQuery)
		if err != nil {
			logger.Warn(fmt.Sprintf("failed to compile permissions claim query '%s': %v", cfg.PermissionsClaimQuery, err))
		}
	}

	if permissionsClaimName != "" && permissionsClaimQuery != nil {
		logger.Warn("both permissionsClaimName and permissionsClaimQuery are set; permissionsClaimName takes precedence")
	}

	if permissionsClaimName == "" && permissionsClaimQuery == nil {
		logger.Info(fmt.Sprintf("using default permissions claim name: %s", defaultPermissionsClaimName))
		permissionsClaimName = defaultPermissionsClaimName
	}

	var permissionsRegex *regexp.Regexp
	var namespaceIndex, roleIndex int
	if cfg.PermissionsRegex != "" {
		r, err := regexp.Compile(cfg.PermissionsRegex)
		if err == nil {
			for i, name := range r.SubexpNames() {
				switch name {
				case "namespace":
					namespaceIndex = i
				case "role":
					roleIndex = i
				}
			}
			if namespaceIndex != 0 && roleIndex != 0 {
				permissionsRegex = r
			} else {
				logger.Warn("permissions regex does not have namespace or role named group")
			}
		} else {
			logger.Warn(fmt.Sprintf("failed to compile permissions regex '%s': %v", cfg.PermissionsRegex, err))
		}
	}
	return &defaultJWTClaimMapper{
		keyProvider:           provider,
		logger:                logger,
		permissionsClaimName:  permissionsClaimName,
		permissionsClaimQuery: permissionsClaimQuery,
		permissionsRegex:      permissionsRegex,
		matchNamespaceIndex:   namespaceIndex,
		matchRoleIndex:        roleIndex,
	}
}

var _ ClaimMapper = (*defaultJWTClaimMapper)(nil)

func (a *defaultJWTClaimMapper) GetClaims(authInfo *AuthInfo) (*Claims, error) {

	claims := Claims{AuthType: "jwt"}

	if authInfo.AuthToken == "" {
		return &claims, nil
	}

	// We use strings.SplitN even though we check the length later, to avoid
	// unnecessary allocations if the format is correct.
	parts := strings.SplitN(authInfo.AuthToken, " ", 2)
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
	permissions, ok, err := a.extractPermissionsClaim(jwtClaims)
	if err != nil {
		return nil, err
	}
	if ok {
		err := a.extractPermissions(permissions, &claims)
		if err != nil {
			return nil, err
		}
	}
	return &claims, nil
}

func (a *defaultJWTClaimMapper) extractPermissionsClaim(jwtClaims jwt.MapClaims) ([]any, bool, error) {
	if a.permissionsClaimName == "" && a.permissionsClaimQuery == nil {
		return nil, false, fmt.Errorf("neither permissionsClaimName nor permissionsClaimQuery is configured")
	}

	if a.permissionsClaimName != "" {
		if permissions, ok := jwtClaims[a.permissionsClaimName].([]any); ok {
			return permissions, true, nil
		}
	}

	if a.permissionsClaimQuery == nil {
		return nil, false, nil
	}

	value, err := a.permissionsClaimQuery.Search(map[string]any(jwtClaims))
	if err != nil {
		a.logger.Warn(fmt.Sprintf("permissions claim query failed: %v", err))
		return nil, false, serviceerror.NewPermissionDenied("invalid permissions claim query", "")
	}

	permissions, ok := value.([]any)
	if !ok {
		a.logger.Warn("permissions claim query did not return an array")
		return nil, false, nil
	}
	return permissions, true, nil
}

func (a *defaultJWTClaimMapper) extractPermissions(permissions []any, claims *Claims) error {
	for _, permission := range permissions {
		p, ok := permission.(string)
		if !ok {
			a.logger.Warn(fmt.Sprintf("ignoring permission that is not a string: %v", permission))
			continue
		}
		var parts []string
		if a.permissionsRegex != nil {
			match := a.permissionsRegex.FindStringSubmatch(p)
			if len(match) == 0 {
				a.logger.Warn(fmt.Sprintf("ignoring permission not matching pattern: %v", permission))
				continue
			}
			parts = []string{match[a.matchNamespaceIndex], match[a.matchRoleIndex]}
		} else {
			parts = strings.SplitN(p, ":", 2)
			if len(parts) != 2 {
				a.logger.Warn(fmt.Sprintf("ignoring permission in unexpected format: %v", permission))
				continue
			}
		}
		namespace := parts[0]
		if namespace == permissionScopeSystem {
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
		keyFunc = func(token *jwt.Token) (any, error) {
			// reserve context
			// impl may introduce network request to get public key
			return provider.GetKey(context.Background(), token)
		}
	} else {
		keyFunc = func(token *jwt.Token) (any, error) {
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
