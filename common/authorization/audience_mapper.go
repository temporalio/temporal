package authorization

import (
	"context"

	"go.temporal.io/server/common/config"
	"google.golang.org/grpc"
)

// AudienceMapper is a simple implementation of JWTAudienceMapper that returns the configured audience string.
type AudienceMapper struct {
	JwtAudience string
}

// Audience returns the configured audience string.
func (m *AudienceMapper) Audience(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) string {
	return m.JwtAudience
}

// NewAudienceMapper returns a JWTAudienceMapper that always returns the given audience string.
func NewAudienceMapper(audience string) JWTAudienceMapper {
	return &AudienceMapper{JwtAudience: audience}
}

// GetAudienceMapperFromConfig returns a JWTAudienceMapper based on the provided Authorization config.
// Currently, it returns a static audience mapper using the Audience field.
func GetAudienceMapperFromConfig(cfg *config.Authorization) (JWTAudienceMapper, error) {
	return NewAudienceMapper(cfg.Audience), nil
}
