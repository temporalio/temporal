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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
)

type CertChain struct {
	CertPubFile string
	CertKeyFile string
	CaPubFile   string
}

func CertFilePath(dir string) string {
	return dir + "/cert_pub.pem"
}

func KeyFilePath(dir string) string {
	return dir + "/cert_priv.pem"
}

func CAFilePath(dir string) string {
	return dir + "/ca_pub.pem"
}

func ConvertFileToBase64(file string) string {
	fileBytes, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return base64.StdEncoding.EncodeToString(fileBytes)
}

func GenerateTestChain(tempDir string, commonName string) (CertChain, error) {

	chain, _, err := GenerateTestChainWithSN(tempDir, commonName, 0)
	return chain, err
}

func GenerateTestChainWithSN(tempDir string, commonName string, serialNumber int64,
) (CertChain, *tls.Certificate, error) {

	caPubFile := CAFilePath(tempDir)
	certPubFile := CertFilePath(tempDir)
	certPrivFile := KeyFilePath(tempDir)

	caCert, err := GenerateSelfSignedCA(caPubFile)
	if err != nil {
		return CertChain{}, nil, err
	}

	if _, err = GenerateServerCert(caCert, commonName, serialNumber, certPubFile, certPrivFile); err != nil {
		return CertChain{}, nil, err
	}

	return CertChain{CaPubFile: caPubFile, CertPubFile: certPubFile, CertKeyFile: certPrivFile}, caCert, err
}

func GenerateTestCerts(tempDir string, commonName string, num int) ([]*tls.Certificate, *x509.CertPool, *x509.CertPool, error) {

	caCert, err := GenerateSelfSignedCA(CAFilePath(tempDir))
	if err != nil {
		return nil, nil, nil, err
	}
	caPool, err := GenerateSelfSignedCAPool(caCert)
	if err != nil {
		return nil, nil, nil, err
	}

	chains := make([]*tls.Certificate, num)
	for i := 0; i < num; i++ {
		certPubFile := tempDir + fmt.Sprintf("/cert_pub_%d.pem", i)
		certPrivFile := tempDir + fmt.Sprintf("/cert_priv_%d.pem", i)
		cert, err := GenerateServerCert(caCert, commonName, int64(i+100), certPubFile, certPrivFile)
		if err != nil {
			return nil, nil, nil, err
		}
		chains[i] = cert
	}

	wrongCACert, err := GenerateSelfSignedCA(CAFilePath(tempDir))
	if err != nil {
		return nil, nil, nil, err
	}

	wrongCAPool, err := GenerateSelfSignedCAPool(wrongCACert)

	return chains, caPool, wrongCAPool, err
}

func GenerateSelfSignedCAPool(caCert *tls.Certificate) (*x509.CertPool, error) {
	caPEM := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Certificate[0],
	}
	caPool := x509.NewCertPool()
	caPem, err := pemEncodeToBytes(caPEM)
	if err != nil {
		return nil, err
	}
	caPool.AppendCertsFromPEM(caPem)
	return caPool, nil
}

func GenerateSelfSignedCA(filePath string) (*tls.Certificate, error) {
	caCert, err := generateSelfSignedX509CA("undefined", nil, 512)
	if err != nil {
		return nil, err
	}

	pemEncodeToFile(filePath, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Certificate[0],
	})
	return caCert, nil
}

func GenerateServerCert(
	caCert *tls.Certificate,
	commonName string,
	serialNumber int64,
	certPubFile string,
	certPrivFile string,
) (*tls.Certificate, error) {

	serverCert, privKey, err := generateServerX509UsingCAAndSerialNumber(commonName, serialNumber, caCert)
	if err != nil {
		return nil, err
	}

	certPEM := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCert.Certificate[0],
	}
	pemEncodeToFile(certPubFile, certPEM)

	keyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	}
	err = pemEncodeToFile(certPrivFile, keyPEM)
	if err != nil {
		return nil, err
	}

	certPem, err := pemEncodeToBytes(certPEM)
	if err != nil {
		return nil, err
	}

	keyPem, err := pemEncodeToBytes(keyPEM)
	if err != nil {
		return nil, err
	}

	cert, err := tls.X509KeyPair(certPem, keyPem)
	return &cert, err
}

func pemEncodeToFile(file string, block *pem.Block) error {
	bytes, err := pemEncodeToBytes(block)
	if err != nil {
		return err
	}
	return os.WriteFile(file, bytes, os.FileMode(0644))
}

func pemEncodeToBytes(block *pem.Block) ([]byte, error) {
	pemBuffer := new(bytes.Buffer)
	err := pem.Encode(pemBuffer, block)
	if err != nil {
		return nil, err
	}

	return pemBuffer.Bytes(), nil
}
