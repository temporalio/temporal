package encryption

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestAppendError(t *testing.T) {
	assert := assert.New(t)
	err1 := errors.New("error1")
	err2 := errors.New("error2")

	err := appendError(nil, err1)
	assert.Equal(err1, err)
	assert.Equal("error1", err.Error())
	err = appendError(err1, nil)
	assert.Equal(err1, err)
	assert.Equal("error1", err.Error())

	err = appendError(err1, err2)
	assert.Equal(err2, errors.Unwrap(err))
	assert.Equal("error1, error2", err.Error())
}

func TestCABundleTracksEveryCertificate(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	newCA := func(serial int64) []byte {
		template := &x509.Certificate{
			SerialNumber:          big.NewInt(serial),
			Subject:               pkix.Name{CommonName: "ca"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			IsCA:                  true,
			BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		require.NoError(t, err)
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	first, second, renewed := newCA(1), newCA(2), newCA(3)
	oldBundle := append(append([]byte{}, first...), second...)
	newBundle := append(append([]byte{}, first...), renewed...)

	provider := &localStoreCertProvider{logger: log.NewNoopLogger()}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	load := func(bundle []byte) (fromFile, fromData *certCache) {
		require.NoError(t, os.WriteFile(caFile, bundle, 0o600))
		_, fileCerts, err := provider.buildCAPoolFromFiles([]string{caFile})
		require.NoError(t, err)
		_, dataCerts, err := buildCAPoolFromData([]string{base64.StdEncoding.EncodeToString(bundle)})
		require.NoError(t, err)
		return &certCache{clientCACerts: fileCerts}, &certCache{clientCACerts: dataCerts}
	}

	oldFile, oldData := load(oldBundle)
	newFile, newData := load(newBundle)

	assert.Len(t, oldFile.clientCACerts, 2)
	assert.Len(t, oldData.clientCACerts, 2)
	assert.False(t, oldFile.isEqual(newFile))
	assert.False(t, oldData.isEqual(newData))

	// Blocks that the cert pool ignores must not make loading fail.
	invalid := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("invalid")})
	mixed, _ := load(append(append([]byte{}, oldBundle...), invalid...))
	assert.True(t, oldFile.isEqual(mixed))
}
