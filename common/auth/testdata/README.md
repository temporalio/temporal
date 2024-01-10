# TLS Test Data
This directory contains various TLS-related files generated using OpenSSL. The set includes private keys, valid certificates, and intentionally invalid certificates for testing purposes.

## Overview
- **Important**: The `localhost.cnf` file, which contains configuration details for the localhost server, cannot be regenerated as it was manually created.
- Private keys (`*.key`) are RSA keys. Exact regeneration commands are unspecified, but their recreation may not be necessary.
- Certificates (`*.crt`) include a valid Certificate Authority (CA), a valid server certificate, an invalid CA, and an invalid server certificate.

## Private Keys Generation
Several RSA private keys were initially generated and are stored in `*.key` files. The specific configuration of these keys is not documented, but they were likely generated with commands similar to the following:

```shell
openssl genrsa -out ca.key 2048
openssl genrsa -out invalid_ca.key 2048
openssl genrsa -out localhost.key 2048
openssl genrsa -out invalid_localhost.key 2048
```

**Note**: Regenerating private keys should not be necessary for testing, as the relevant information is embedded within the certificates.

## Certificates

### Valid Certificate Authority (CA)
- **File**: `ca.crt`
- **Description**: Self-signed CA certificate.
- **Regeneration Command**:
  ```shell
  # Generate a new CSR for the CA
  openssl req -new -key ca.key -out ca.csr -nodes -batch

  # Self-Sign the new CA certificate
  openssl x509 -req -in ca.csr -signkey ca.key -out ca.crt -days 36500 -sha256

  # Verify the new CA certificate
  openssl verify -CAfile ca.crt ca.crt

  # Clean up the CSR
  rm ca.csr
  ```
  **Note**: The CA is set with a 100-year expiry to avoid frequent regeneration.

### Valid Server Certificate
- **File**: `localhost.crt`
- **Description**: Server certificate signed by the valid CA.
- **Regeneration Command**:
  ```shell
  openssl req -new -key localhost.key -out localhost.csr -config localhost.cnf
  openssl x509 -req -in localhost.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out localhost.crt -days 36500 -sha256 -extensions req_ext -extfile localhost.cnf
  openssl verify -CAfile ca.crt localhost.crt
  rm ca.srl localhost.csr
  ```
  **Note**: Includes `127.0.0.1` as an IP SAN. Regeneration or resigning is not required if the CA certificate changes but its private key remains the same.

### Invalid CA
- **File**: `invalid_ca.crt`
- **Description**: Another CA certificate that client systems should not trust.
- **Regeneration Command**:
  ```shell
  openssl req -new -key invalid_ca.key -out invalid_ca.csr -nodes -batch
  openssl x509 -req -in invalid_ca.csr -signkey invalid_ca.key -out invalid_ca.crt -days 36500 -sha256
  openssl verify -CAfile invalid_ca.crt invalid_ca.crt
  rm invalid_ca.csr
  ```

### Invalid Server Certificate
- **File**: `invalid_localhost.crt`
- **Description**: A self-signed server certificate expected to be rejected by clients.
- **Regeneration Command**:
  ```shell
  openssl req -new -key invalid_localhost.key -out invalid_localhost.csr -config localhost.cnf
  openssl x509 -req -in invalid_localhost.csr -signkey invalid_localhost.key -out invalid_localhost.crt -days 36500 -sha256
  openssl verify -CAfile invalid_localhost.crt invalid_localhost.crt
  rm invalid_localhost.csr
  ```
