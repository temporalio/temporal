# Run MySQL v5.7 on macOS

### Install
```bash
brew install mysql@5.7
```

### Start
```bash
brew services start mysql@5.7
```

### Stop
```bash
brew services stop mysql@5.7
```

### Post Installation
Verify MySQL v5.7 is running and accessible:
```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

Within `mysql` shell, create user and password:
```mysql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';
CREATE USER 'temporal'@'localhost' IDENTIFIED BY 'temporal';
GRANT ALL PRIVILEGES ON *.* TO 'temporal'@'localhost';
```

Verify password:
```bash
mysql -h 127.0.0.1 -P 3306 -u root -p
mysql -h 127.0.0.1 -P 3306 -u temporal -p
```

### TLS
[TLS Key / Cert Setup Guide](../tls/tls.md)

```bash
emacs /usr/local/etc/my.cnf
```

setting the variables below to
```
require_secure_transport=ON
ssl-ca=<path to the server-cert.pem>
ssl-cert=<path to the server-cert.pem>
ssl-key=<path to the server-key.pem>
```

```mysql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root' REQUIRE X509;
ALTER USER 'temporal'@'localhost' IDENTIFIED BY 'temporal' REQUIRE X509;
```

then restart MySQL:
```bash
brew services restart mysql@5.7
```

Verify TLS & password:
```bash
mysql -u root -p \
  --ssl-cert=<path to the client-cert.pem> \
  --ssl-key=<path to the client-key.pem> \
  --ssl-ca=<path to the ca.pem>
```