package frontend

import (
	"net/http"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc/credentials"
)

// nexusRejectionLog returns the log function for a Nexus HTTP request rejected as malformed: warn when the
// request was sent by a principal other than a Temporal server, error otherwise.
//
// Most Nexus HTTP requests are built by a Temporal server: callers' history services start and cancel
// operations, and handlers' history services deliver completions. A malformed request from a server points
// at a server bug (INC-1537 surfaced this way), so it stays at error. Any other principal, such as an
// external handler delivering a completion or a client calling an endpoint directly, is the source of its
// own mistake. When the sender can't be established, including when authorization isn't configured, the
// request is treated as server-sent.
//
// Rejections are resolved before the request is authorized, so the sender is resolved here, on the failure
// path only.
func nexusRejectionLog(auth *authorization.Interceptor, r *http.Request, logger log.Logger) func(msg string, tags ...tag.Tag) {
	if sentByNonServerPrincipal(auth, r) {
		return logger.Warn
	}
	return logger.Error
}

func sentByNonServerPrincipal(auth *authorization.Interceptor, r *http.Request) bool {
	if auth == nil {
		return false
	}
	authInfo := auth.GetAuthInfo(tlsInfoFromRequest(r), r.Header, func() string {
		return "" // TODO: support audience getter
	})
	if authInfo == nil {
		return false
	}
	claims, err := auth.GetClaims(authInfo)
	if err != nil || claims == nil {
		return false
	}
	return claims.System == authorization.RoleUndefined
}

func tlsInfoFromRequest(r *http.Request) *credentials.TLSInfo {
	if r.TLS == nil {
		return nil
	}
	return &credentials.TLSInfo{
		State:          *r.TLS,
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
	}
}
