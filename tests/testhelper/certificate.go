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

package testhelper

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	mathrand "math/rand"
	"net"
	"strings"
	"time"
)

// GenerateSelfSignedX509CA generates a TLS serverCert that is self-signed
func generateSelfSignedX509CA(commonName string, extUsage []x509.ExtKeyUsage, keyLengthBits int) (*tls.Certificate, error) {
	now := time.Now().UTC()

	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.Unix()),
		Subject: pkix.Name{
			CommonName:   commonName,
			Country:      []string{"USA"},
			Organization: []string{"TemporalTechnologiesTesting"},
		},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.AddDate(0, 0, 1), // 1 day expiry
		BasicConstraintsValid: true,
		IsCA:                  true,
		ExtKeyUsage:           extUsage,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDigitalSignature,
	}

	if ip := net.ParseIP(commonName).To4(); ip != nil {
		template.IPAddresses = []net.IP{ip}

		if ip.IsLoopback() {
			template.DNSNames = []string{"localhost"}
		}
	}

	if strings.ToLower(commonName) == "localhost" {
		template.IPAddresses = []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)}
		template.DNSNames = []string{"localhost"}
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, keyLengthBits)
	if err != nil {
		return &tls.Certificate{}, err
	}

	cert, err := x509.CreateCertificate(rand.Reader, template, template, privateKey.Public(), privateKey)
	if err != nil {
		return &tls.Certificate{}, err
	}

	var tlsCert tls.Certificate
	tlsCert.Certificate = append(tlsCert.Certificate, cert)
	tlsCert.PrivateKey = privateKey

	return &tlsCert, nil
}

// GenerateServerX509UsingCA generates a TLS serverCert that is self-signed
func generateServerX509UsingCAAndSerialNumber(commonName string, serialNumber int64, ca *tls.Certificate) (*tls.Certificate, *rsa.PrivateKey, error) {
	now := time.Now().UTC()

	i := serialNumber
	if i == 0 {
		i = mathrand.Int63n(100000000000000000)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(i),
		Subject: pkix.Name{
			CommonName:   commonName,
			Country:      []string{"USA"},
			Organization: []string{"TemporalTechnologiesTesting"},
		},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.AddDate(0, 0, 1), // 1 day expiry
		BasicConstraintsValid: true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature,
	}

	if ip := net.ParseIP(commonName).To4(); ip != nil {
		template.IPAddresses = []net.IP{ip}

		if ip.IsLoopback() {
			template.DNSNames = []string{"localhost"}
		}
	}

	if strings.ToLower(commonName) == "localhost" {
		template.IPAddresses = []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)}
		template.DNSNames = []string{"localhost"}
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return &tls.Certificate{}, nil, err
	}

	caCert, err := x509.ParseCertificate(ca.Certificate[0])
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.CreateCertificate(rand.Reader, template, caCert, privateKey.Public(), ca.PrivateKey)
	if err != nil {
		return &tls.Certificate{}, nil, err
	}

	var tlsCert tls.Certificate
	tlsCert.Certificate = append(tlsCert.Certificate, cert)
	tlsCert.PrivateKey = privateKey

	return &tlsCert, privateKey, err
}
