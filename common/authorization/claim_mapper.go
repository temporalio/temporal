//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination claim_mapper_mock.go

package authorization

import (
	"crypto/x509/pkix"
	"fmt"
	"strings"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc/credentials"
)

// @@@SNIPSTART temporal-common-authorization-authinfo
// Authentication information from subject's JWT token or/and mTLS certificate
type AuthInfo struct {
	AuthToken     string
	TLSSubject    *pkix.Name
	TLSConnection *credentials.TLSInfo
	ExtraData     string
	Audience      string
}

// @@@SNIPEND

// @@@SNIPSTART temporal-common-authorization-claimmapper-interface
// ClaimMapper converts authorization info of a subject into Temporal claims (permissions) for authorization
type ClaimMapper interface {
	GetClaims(authInfo *AuthInfo) (*Claims, error)
}

// @@@SNIPEND

// Normally, GetClaims will never be called without either an auth token or TLS metadata set in
// AuthInfo. However, if you want your ClaimMapper to be called in all cases, you can implement
// this additional interface and return false.
type ClaimMapperWithAuthInfoRequired interface {
	AuthInfoRequired() bool
}

// No-op claim mapper that gives system level admin permission to everybody
type noopClaimMapper struct{}

var _ ClaimMapper = (*noopClaimMapper)(nil)
var _ ClaimMapperWithAuthInfoRequired = (*noopClaimMapper)(nil)

func NewNoopClaimMapper() ClaimMapper {
	return &noopClaimMapper{}
}

func (*noopClaimMapper) GetClaims(_ *AuthInfo) (*Claims, error) {
	return &Claims{System: RoleAdmin}, nil
}

// This implementation can run even without auth info.
func (*noopClaimMapper) AuthInfoRequired() bool {
	return false
}

func GetClaimMapperFromConfig(config *config.Authorization, logger log.Logger) (ClaimMapper, error) {

	switch strings.ToLower(config.ClaimMapper) {
	case "":
		return NewNoopClaimMapper(), nil
	case "default":
		return NewDefaultJWTClaimMapper(NewDefaultTokenKeyProvider(config, logger), config, logger), nil
	}
	return nil, fmt.Errorf("unknown claim mapper: %s", config.ClaimMapper)
}
