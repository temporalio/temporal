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

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v4"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/multierr"
)

// Default token key provider
type defaultTokenKeyProvider struct {
	config   config.JWTKeyProvider
	rsaKeys  map[string]*rsa.PublicKey
	ecKeys   map[string]*ecdsa.PublicKey
	keysLock sync.RWMutex
	ticker   *time.Ticker
	logger   log.Logger
	stop     chan bool
}

var _ TokenKeyProvider = (*defaultTokenKeyProvider)(nil)

func NewDefaultTokenKeyProvider(cfg *config.Authorization, logger log.Logger) *defaultTokenKeyProvider {
	provider := defaultTokenKeyProvider{config: cfg.JWTKeyProvider, logger: logger}
	provider.initialize()
	return &provider
}

func (a *defaultTokenKeyProvider) initialize() {
	a.rsaKeys = make(map[string]*rsa.PublicKey)
	a.ecKeys = make(map[string]*ecdsa.PublicKey)
	if a.config.HasSourceURIsConfigured() {
		err := a.updateKeys()
		if err != nil {
			a.logger.Error("error during initial retrieval of token keys: ", tag.Error(err))
		}
	}
	if a.config.RefreshInterval > 0 {
		a.stop = make(chan bool)
		a.ticker = time.NewTicker(a.config.RefreshInterval)
		go a.timerCallback()
	}
}

func (a *defaultTokenKeyProvider) Close() {
	a.ticker.Stop()
	a.stop <- true
	close(a.stop)
}

func (a *defaultTokenKeyProvider) RsaKey(alg string, kid string) (*rsa.PublicKey, error) {
	if !strings.EqualFold(alg, jwt.SigningMethodRS256.Name) {
		return nil, fmt.Errorf("unexpected signing algorithm: %s", alg)
	}

	a.keysLock.RLock()
	key, found := a.rsaKeys[kid]
	a.keysLock.RUnlock()
	if !found {
		return nil, fmt.Errorf("RSA key not found for key ID: %s", kid)
	}
	return key, nil
}

func (a *defaultTokenKeyProvider) EcdsaKey(alg string, kid string) (*ecdsa.PublicKey, error) {
	if !strings.EqualFold(alg, jwt.SigningMethodES256.Name) {
		return nil, fmt.Errorf("unexpected signing algorithm: %s", alg)
	}

	a.keysLock.RLock()
	key, found := a.ecKeys[kid]
	a.keysLock.RUnlock()
	if !found {
		return nil, fmt.Errorf("ECDSA key not found for key ID: %s", kid)
	}
	return key, nil
}

func (a *defaultTokenKeyProvider) SupportedMethods() []string {
	return []string{jwt.SigningMethodRS256.Name, jwt.SigningMethodES256.Name}
}

func (a *defaultTokenKeyProvider) timerCallback() {
	for {
		select {
		case <-a.stop:
			return
		case <-a.ticker.C:
		}
		if a.config.HasSourceURIsConfigured() {
			err := a.updateKeys()
			if err != nil {
				a.logger.Error("error while refreshing token keys: ", tag.Error(err))
			}
		}
	}
}

func (a *defaultTokenKeyProvider) updateKeys() error {
	if !a.config.HasSourceURIsConfigured() {
		return fmt.Errorf("no URIs configured for retrieving token keys")
	}

	rsaKeys := make(map[string]*rsa.PublicKey)
	ecKeys := make(map[string]*ecdsa.PublicKey)

	for _, uri := range a.config.KeySourceURIs {
		if strings.TrimSpace(uri) == "" {
			continue
		}
		err := a.updateKeysFromURI(uri, rsaKeys, ecKeys)
		if err != nil {
			return err
		}
	}
	// swap old keys with the new ones
	a.keysLock.Lock()
	a.rsaKeys = rsaKeys
	a.ecKeys = ecKeys
	a.keysLock.Unlock()
	return nil
}

func (a *defaultTokenKeyProvider) updateKeysFromURI(
	uri string,
	rsaKeys map[string]*rsa.PublicKey,
	ecKeys map[string]*ecdsa.PublicKey,
) (err error) {

	resp, err := http.Get(uri)
	if err != nil {
		return err
	}
	defer func() {
		err = multierr.Combine(err, resp.Body.Close())
	}()

	jwks := jose.JSONWebKeySet{}
	err = json.NewDecoder(resp.Body).Decode(&jwks)
	if err != nil {
		return err
	}

	for _, k := range jwks.Keys {
		switch k.Key.(type) {
		case *rsa.PublicKey:
			rsaKeys[k.KeyID] = k.Key.(*rsa.PublicKey)
		case *ecdsa.PublicKey:
			ecKeys[k.KeyID] = k.Key.(*ecdsa.PublicKey)
		default:
			a.logger.Warn(fmt.Sprintf("unexpected type of JWKS public key %s", k.Algorithm))
		}
	}
	return nil
}

func (a *defaultTokenKeyProvider) HmacKey(alg string, kid string) ([]byte, error) {
	return nil, fmt.Errorf("unsupported key type HMAC for: %s", alg)
}
