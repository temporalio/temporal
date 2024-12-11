# Run TiDB Latest Version on macOS

### Install

```bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
```

Or, you can refer this document of [deploying the TiDB database](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb).

### Start

```bash
tiup playground --db.config config.toml
```

> **Note:**
>
> In this way, you will get a test/dev TiDB instance. However, if you want to deploy a formal cluster of TiDB, please refer to [this document](https://docs.pingcap.com/tidb/stable/production-deployment-using-tiup).

### Stop

If you started the instance by `tiup playground`, just stop the terminal.

### Post Installation

Verify TiDB is running and accessible:

```bash
mysql -h 127.0.0.1 -P 4000 -u root
```

Within `mysql` shell, create user and password:
```mysql
CREATE USER 'temporal'@'%' IDENTIFIED BY 'temporal';
GRANT ALL PRIVILEGES ON *.* TO 'temporal'@'%';
FLUSH PRIVILEGES;
```

Verify password:

```bash
mysql -h 127.0.0.1 -P 3306 -u temporal -p
```

### TLS

For TiDB, you can use [`auto-tls`](https://docs.pingcap.com/tidb/stable/tidb-configuration-file#auto-tls) variable to automatically generate the TLS certificates on startup.

Then, you can create a user who is required to use X.509 certificate or TLS connection. You can refer [this document](https://docs.pingcap.com/tidb/stable/sql-statement-create-use) to get more information.

```mysql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root'  REQUIRE ISSUER '/C=US/ST=California/L=San Francisco/O=PingCAP';
ALTER USER 'temporal'@'localhost' IDENTIFIED BY 'temporal' REQUIRE SSL;
```