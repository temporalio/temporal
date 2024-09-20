# Temporal Server TLS Key / Cert Setup Guide

## Setup Local CA
```bash
openssl genrsa -aes256 -out ca-key.pem 4096
openssl req -new -x509 -days 365 -key ca-key.pem -sha256 -out ca.pem
```

## Setup Key / Cert for Callee. e.g. Database
```bash
openssl genrsa -out server-key.pem 4096
openssl req -subj "/CN=localhost" -sha256 -new -key server-key.pem -out server.csr
echo subjectAltName = IP:127.0.0.1,DNS:localhost > extfile.cnf
openssl x509 -req -days 365 -sha256 -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile extfile.cnf
```

## Setup Key / Cert for Caller. e.g. Server
```bash
openssl genrsa -out client-key.pem 4096
openssl req -subj '/CN=localhost' -new -key client-key.pem -out client.csr
echo subjectAltName = IP:127.0.0.1,DNS:localhost > extfile.cnf
openssl x509 -req -days 365 -sha256 -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem -extfile extfile.cnf
```

## Cleanup & Permission
```bash
rm -v client.csr server.csr

chmod -v 0400 ca-key.pem server-key.pem client-key.pem 
chmod -v 0444 ca.pem server-cert.pem client-cert.pem
```
