package testutils

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

	if ip := net.ParseIP(commonName); ip != nil {
		if ip.IsLoopback() {
			template.IPAddresses = []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)}
			template.DNSNames = []string{"localhost"}
		} else {
			template.IPAddresses = []net.IP{ip}
		}
	} else {
		template.DNSNames = []string{commonName}
	}

	if strings.ToLower(commonName) == "localhost" {
		template.IPAddresses = []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)}
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

	if ip := net.ParseIP(commonName); ip != nil {
		if ip.IsLoopback() {
			template.IPAddresses = []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)}
			template.DNSNames = []string{"localhost"}
		} else {
			template.IPAddresses = []net.IP{ip}
		}
	} else {
		template.DNSNames = []string{commonName}
	}

	if strings.ToLower(commonName) == "localhost" {
		template.IPAddresses = []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)}
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
