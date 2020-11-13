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
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dgrijalva/jwt-go"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/service/config"
)

type jwks struct {
	Keys []jsonWebKeys `json:"keys"`
}

type jsonWebKeys struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

// Default RSA token key provider
type rsaKeyProvider struct {
	config   config.JWTKeyProvider
	keys     map[string]*rsa.PublicKey
	keysLock sync.RWMutex
	timer    *time.Timer
	logger   log.Logger
}

var _ TokenKeyProvider = (*rsaKeyProvider)(nil)

func NewRSAKeyProvider(cfg *config.Config) *rsaKeyProvider {
	logger := loggerimpl.NewLogger(cfg.Log.NewZapLogger())
	provider := rsaKeyProvider{config: cfg.Global.Security.JWTKeyProvider, logger: logger}
	provider.init()
	return &provider
}

func (a *rsaKeyProvider) init() {
	a.keys = make(map[string]*rsa.PublicKey)
	if len(a.config.KeySourceURIs) > 0 {
		err := a.updateKeys()
		if err != nil {
			a.logger.Error("error during initial retrieval of RSA token keys: ", tag.Error(err))
		}
	}
	a.timer = time.AfterFunc(a.config.RefreshTime, func() {
		a.timerCallback()
	})
}

func (a *rsaKeyProvider) rsaKey(alg string, kid string) (*rsa.PublicKey, error) {
	if !strings.EqualFold(alg, "rs256") {
		return nil, fmt.Errorf("unexpected signing algorithm: %s", alg)
	}

	a.keysLock.RLock()
	key, found := a.keys[kid]
	a.keysLock.RUnlock()
	if !found {
		return nil, fmt.Errorf("RSA key not found for key ID: %s", kid)
	}
	return key, nil
}

func (a *rsaKeyProvider) timerCallback() {
	if len(a.config.KeySourceURIs) > 0 {
		err := a.updateKeys()
		if err != nil {
			a.logger.Error("error while refreshing RSA token keys: ", tag.Error(err))
		}
	}
	a.timer = time.AfterFunc(a.config.RefreshTime, func() {
		a.timerCallback()
	})
}

func (a *rsaKeyProvider) updateKeys() error {
	if len(a.config.KeySourceURIs) == 0 {
		return fmt.Errorf("no URIs configured for retrieving keys")
	}

	keys := make(map[string]*rsa.PublicKey)

	for _, uri := range a.config.KeySourceURIs {
		resp, err := http.Get(uri)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		jwks := jwks{}
		err = json.NewDecoder(resp.Body).Decode(&jwks)
		if err != nil {
			return err
		}

		for _, k := range jwks.Keys {
			cert := "-----BEGIN CERTIFICATE-----\n" + k.X5c[0] + "\n-----END CERTIFICATE-----"
			key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
			if err != nil {
				return err
			}
			keys[k.Kid] = key
		}
	}
	// swap old keys with the new ones
	a.keysLock.Lock()
	a.keys = keys
	a.keysLock.Unlock()
	return nil
}

func (a *rsaKeyProvider) hmacKey(alg string, kid string) ([]byte, error) {
	return nil, fmt.Errorf("unsupported key type HMAC for: %s", alg)
}

func (a *rsaKeyProvider) ecdsaKey(alg string, kid string) (*ecdsa.PublicKey, error) {
	return nil, fmt.Errorf("unsupported key type ECDSA for: %s", alg)
}
