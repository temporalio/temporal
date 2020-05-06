// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

package encryption

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/temporalio/temporal/common/service/config"
)


type (
	// TLSFactory creates certificates based on Encryption internodeSettings
	TLSFactory interface {
		GenerateInternodeServerConfig() (*tls.Config, error)
		GenerateInternodeClientConfig() (*tls.Config, error)
		GenerateFrontendServerConfig() (*tls.Config, error)
		GenerateFrontendClientConfig() (*tls.Config, error)
	}

	CertProvider interface {
		FetchServerCertificate() (*tls.Certificate, error)
		FetchClientCAs () (*x509.CertPool, error)
		FetchServerRootCAsForClient() (*x509.CertPool, error)
		GetSettings() (*config.GroupTLS)
	}

	// GRPCDialer creates gRPC connection.
	tlsGenerator func(localProvider CertProvider, settingsProvider CertProvider) (*tls.Config, error)

	ProviderType string
)

const (
	ProviderTypeLocalStore ProviderType = "localstore"
	ProviderTypeSelfSigned ProviderType = "selfsigned"
)

func NewTLSFactoryFromConfig(encryptionSettings *config.RootTLS, hostname string) (TLSFactory, error) {
	if encryptionSettings == nil /*|| encryptionSettings.Provider == "" */{
		return nil, nil
	}

	/*switch ProviderType(encryptionSettings.Provider) {
	case ProviderTypeSelfSigned:
		return NewSelfSignedTlsFactory(encryptionSettings, hostname)
	case ProviderTypeLocalStore:*/
	return NewLocalStoreTlsFactory(encryptionSettings)
	//}

	//return nil, fmt.Errorf("unknown provider: %v", encryptionSettings.Provider)
}



