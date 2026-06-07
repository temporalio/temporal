package principaltoken

import (
	"net/http"
)

// JWKSHandler returns an HTTP handler that serves this cluster's public
// verification keys as a JWKS document. Peer clusters fetch it to verify tokens
// this cluster mints. It exposes only public keys and is safe to serve
// unauthenticated.
func JWKSHandler(kp KeyProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jwks, err := kp.PublicJWKS(r.Context())
		if err != nil {
			http.Error(w, "failed to render JWKS", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = w.Write(jwks)
	}
}
