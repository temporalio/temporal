# Run PostgreSQL v9.6 on macOS

### Install
```bash
brew install postgresql@9.6
```

### Start
```bash
brew services start postgresql@9.6
```

### Stop
```bash
brew services stop postgresql@9.6
```

### Post Installation
Create a user `postgres`
```bash
createuser -s root
```

Verify PostgreSQL v9.6 is running and accessible:
```bash
psql -h 127.0.0.1 -p 5432 -U root -d postgres
```

Within `psql` shell, add a password:
```postgresql
ALTER USER root WITH PASSWORD 'root';
ALTER USER root WITH SUPERUSER;
CREATE USER temporal WITH PASSWORD 'temporal';
ALTER USER temporal WITH SUPERUSER;
```

Change the following file context:
```bash
emacs /usr/local/var/postgresql@9.6/pg_hba.conf
```
from
```
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
```
to
```
local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
```
then restart PostgreSQL:
```bash
brew services restart postgresql@9.6
```

Verify password:
```bash
psql -h 127.0.0.1 -p 5432 -U root -d postgres
psql -h 127.0.0.1 -p 5432 -U temporal -d postgres
```

### TLS
[TLS Key / Cert Setup Guide](../tls/tls.md)

```bash
emacs /usr/local/var/postgresql@9.6/postgresql.conf
```

setting the variables below to
```
ssl=on
ssl_cert_file=<path to the server-cert.pem>
ssl_key_file=<path to the server-key.pem>
ssl_ca_file=<path to the ca.pem>
```

```bash
emacs /usr/local/var/postgresql@9.6/pg_hba.conf
```

changes the configs like below
```
hostssl    all             all             127.0.0.1/32            md5 clientcert=1                                                     
hostssl    all             all             ::1/128                 md5 clientcert=1
```
then restart PostgreSQL:
```bash
brew services restart postgresql@9.6
```

Verify TLS & password:
```bash
psql "sslmode=require host=localhost dbname=postgres user=root \
  sslkey=<path to the client-key.pem> \
  sslcert=<path to the client-cert.pem> \
  sslrootcert=<path to the ca.pem>"
```