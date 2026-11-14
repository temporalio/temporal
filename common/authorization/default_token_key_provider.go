package authorization

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v4"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// keyFetchTimeout bounds a single request for a key set. Without it, an endpoint that accepts the
// connection and never answers blocks server startup and the periodic refresh indefinitely.
// It is a variable so that tests can shorten it.
var keyFetchTimeout = 10 * time.Second

// Default token key provider
type defaultTokenKeyProvider struct {
	config     config.JWTKeyProvider
	rsaKeys    map[string]*rsa.PublicKey
	ecKeys     map[string]*ecdsa.PublicKey
	keysLock   sync.RWMutex
	ticker     *time.Ticker
	logger     log.Logger
	stop       chan bool
	httpClient *http.Client
	// fetchContext is canceled by Close so that an in-flight key request does not delay shutdown.
	fetchContext context.Context
	cancelFetch  context.CancelFunc
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
	a.httpClient = &http.Client{Timeout: keyFetchTimeout}
	a.fetchContext, a.cancelFetch = context.WithCancel(context.Background())
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
	// Cancel first: the refresh goroutine may be inside a key request and could not receive on stop.
	if a.cancelFetch != nil {
		a.cancelFetch()
	}
	// ticker and stop are only set when RefreshInterval > 0, so both may be nil here. Guard them
	// as localStoreCertProvider.Close and localStoreTlsProvider.Close already do.
	if a.ticker != nil {
		a.ticker.Stop()
	}
	if a.stop != nil {
		a.stop <- true
		close(a.stop)
	}
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

	resp, err := a.openURI(uri)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, resp.Close())
	}()

	jwks := jose.JSONWebKeySet{}
	if err := json.NewDecoder(resp).Decode(&jwks); err != nil {
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

// openURI returns a ReadCloser for the given URI. Supports http://, https://, and file:// schemes.
func (a *defaultTokenKeyProvider) openURI(uri string) (io.ReadCloser, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "file" {
		if u.Host != "" && u.Host != "localhost" {
			return nil, fmt.Errorf("file URI with remote host is not supported: %s", uri)
		}
		return os.Open(u.Path)
	}
	request, err := http.NewRequestWithContext(a.fetchContext, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	// Reject error responses: a gateway may answer 5xx with a JSON body that decodes into an empty
	// key set, which would replace the working keys and make every token fail validation.
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected HTTP status %d from %s", resp.StatusCode, u.Host)
	}
	return resp.Body, nil
}

func (a *defaultTokenKeyProvider) HmacKey(alg string, kid string) ([]byte, error) {
	return nil, fmt.Errorf("unsupported key type HMAC for: %s", alg)
}
