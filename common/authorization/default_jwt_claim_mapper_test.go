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
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go/v4"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

type errorTestOptions int16

const (
	errorTestOptionNoKID = errorTestOptions(1 << iota)
	errorTestOptionNoSubject
	errorTestOptionNoAlgorithm
	errorTestOptionNoError = errorTestOptions(0)
)

const (
	testSubject      = "test-user"
	defaultNamespace = "default"
)

var (
	permissionsAdmin              = []string{"system:admin", "default:read"}
	permissionsReaderWriterWorker = []string{"default:read", "default:write", "default:worker"}
)

type (
	defaultClaimMapperSuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		tokenGenerator *tokenGenerator
		claimMapper    ClaimMapper
		config         *config.Authorization
		logger         log.Logger
	}
)

func TestDefaultClaimMapperSuite(t *testing.T) {
	s := new(defaultClaimMapperSuite)
	suite.Run(t, s)
}
func (s *defaultClaimMapperSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.tokenGenerator = newTokenGenerator()
	s.config = &config.Authorization{}
	s.logger = log.NewNoopLogger()
	s.claimMapper = NewDefaultJWTClaimMapper(s.tokenGenerator, s.config, s.logger)
}
func (s *defaultClaimMapperSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *defaultClaimMapperSuite) TestTokenGenerator() {
	tokenString, err := s.tokenGenerator.generateToken(
		testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	claims, err := parseJWT(tokenString, s.tokenGenerator)
	s.NoError(err)
	s.Equal(testSubject, claims["sub"])
}
func (s *defaultClaimMapperSuite) TestTokenWithNoSubject() {
	tokenString, err := s.tokenGenerator.generateToken(
		testSubject, permissionsAdmin, errorTestOptionNoSubject)
	s.NoError(err)
	claims, err := parseJWT(tokenString, s.tokenGenerator)
	s.NoError(err)
	subject := claims["sub"]
	s.Nil(subject)
}
func (s *defaultClaimMapperSuite) TestTokenWithNoKID() {
	tokenString, err := s.tokenGenerator.generateToken(
		testSubject, permissionsAdmin, errorTestOptionNoKID)
	s.NoError(err)
	_, err = parseJWT(tokenString, s.tokenGenerator)
	s.Error(err, "malformed token - no \"kid\" header")
}
func (s *defaultClaimMapperSuite) TestTokenWithNoAlgorithm() {
	tokenString, err := s.tokenGenerator.generateToken(
		testSubject, permissionsAdmin, errorTestOptionNoAlgorithm)
	s.NoError(err)
	_, err = parseJWT(tokenString, s.tokenGenerator)
	s.Error(err, "signing method (alg) is unspecified.")
}
func (s *defaultClaimMapperSuite) TestTokenWithAdminPermissions() {
	tokenString, err := s.tokenGenerator.generateToken(
		testSubject, permissionsAdmin, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
	}
	claims, err := s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleAdmin, claims.System)
	s.Equal(1, len(claims.Namespaces))
	defaultRole := claims.Namespaces[defaultNamespace]
	s.Equal(RoleReader, defaultRole)
}
func (s *defaultClaimMapperSuite) TestTokenWithReaderWriterWorkerPermissions() {
	tokenString, err := s.tokenGenerator.generateToken(
		testSubject, permissionsReaderWriterWorker, errorTestOptionNoError)
	s.NoError(err)
	authInfo := &AuthInfo{
		AddBearer(tokenString),
		nil,
		nil,
		"",
	}
	claims, err := s.claimMapper.GetClaims(authInfo)
	s.NoError(err)
	s.Equal(testSubject, claims.Subject)
	s.Equal(RoleUndefined, claims.System)
	s.Equal(1, len(claims.Namespaces))
	defaultRole := claims.Namespaces[defaultNamespace]
	s.Equal(RoleReader|RoleWriter|RoleWorker, defaultRole)
}
func (s *defaultClaimMapperSuite) TestGetClaimMapperFromConfigNoop() {
	s.testGetClaimMapperFromConfig("", true, reflect.TypeOf(&noopClaimMapper{}))
}
func (s *defaultClaimMapperSuite) TestGetClaimMapperFromConfigDefault() {
	s.testGetClaimMapperFromConfig("default", true, reflect.TypeOf(&defaultJWTClaimMapper{}))
}

func (s *defaultClaimMapperSuite) TestGetClaimMapperFromConfigUnknown() {
	s.testGetClaimMapperFromConfig("foo", false, nil)
}

func (s *defaultClaimMapperSuite) testGetClaimMapperFromConfig(name string, valid bool, cmType reflect.Type) {

	cfg := config.Authorization{}
	cfg.ClaimMapper = name
	cm, err := GetClaimMapperFromConfig(&cfg, s.logger)
	if valid {
		s.NoError(err)
		s.NotNil(cm)
		t := reflect.TypeOf(cm)
		s.True(t == cmType)
	} else {
		s.Error(err)
		s.Nil(cm)
	}
}

func AddBearer(token string) string {
	return "Bearer " + token
}

type (
	tokenGenerator struct {
		privateKey *rsa.PrivateKey
		publicKey  *rsa.PublicKey
	}
)

func newTokenGenerator() *tokenGenerator {

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil
	}

	return &tokenGenerator{privateKey: key, publicKey: &key.PublicKey}
}

type (
	CustomClaims struct {
		Permissions []string `json:"permissions"`
		jwt.StandardClaims
	}
)

func (CustomClaims) Valid(*jwt.ValidationHelper) error {
	return nil
}

func (tg *tokenGenerator) generateToken(subject string, permissions []string, options errorTestOptions) (string, error) {
	claims := CustomClaims{
		permissions,
		jwt.StandardClaims{
			ExpiresAt: jwt.At(time.Now().Add(time.Hour)),
			Issuer:    "test",
		},
	}
	if options&errorTestOptionNoSubject == 0 {
		claims.Subject = subject
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if options&errorTestOptionNoKID == 0 {
		token.Header["kid"] = "test-key"
	}
	if options&errorTestOptionNoAlgorithm > 0 {
		delete(token.Header, "alg")
	}
	signedToken, err := token.SignedString(tg.privateKey)
	return signedToken, err
}

func (tg *tokenGenerator) EcdsaKey(alg string, kid string) (*ecdsa.PublicKey, error) {
	return nil, fmt.Errorf("unsupported key type ECDSA for: %s", alg)
}
func (tg *tokenGenerator) HmacKey(alg string, kid string) ([]byte, error) {
	return nil, fmt.Errorf("unsupported key type HMAC for: %s", alg)
}
func (tg *tokenGenerator) RsaKey(alg string, kid string) (*rsa.PublicKey, error) {
	return tg.publicKey, nil
}
func (tg *tokenGenerator) Close() {
}
