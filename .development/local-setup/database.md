# Temporal Server Local Setup Guide

## Database

### MySQL v5.7

#### Install
```zsh
brew install mysql@5.7
```

#### Start
```zsh
brew services start mysql@5.7
```

#### Stop
```zsh
brew services stop mysql@5.7
```

#### Post Installation
Verify MySQL v5.7 is running & accessible:
```zsh
mysql -h 127.0.0.1 -P 3306 -u root
```

Within mysql shell, create user & password:
```mysql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';
CREATE USER 'temporal'@'localhost' IDENTIFIED BY 'temporal';
GRANT ALL PRIVILEGES ON *.* TO 'temporal'@'localhost';
```

Verify password:
```zsh
mysql -h 127.0.0.1 -P 3306 -u root -p
mysql -h 127.0.0.1 -P 3306 -u temporal -p
```

### PostgreSQL v9.6

#### Install
```zsh
brew install postgresql@9.6
```

#### Start
```zsh
brew services start postgresql@9.6
```

#### Stop
```zsh
brew services stop postgresql@9.6
```

#### Post Installation
Create a user `postgres`
```zsh
createuser -s root
```

Verify PostgreSQL v9.6 is running & accessible:
```zsh
psql -h 127.0.0.1 -p 5432 -U root -d postgres
```

Within psql shell, add a password:
```postgresql
ALTER USER root WITH PASSWORD 'root';
ALTER USER root WITH SUPERUSER;
CREATE USER temporal WITH PASSWORD 'temporal';
ALTER USER temporal WITH SUPERUSER;
```

Change the following file context:
```zsh
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
```zsh
brew services restart postgresql@9.6
```

Verify password:
```zsh
psql -h 127.0.0.1 -p 5432 -U root -d postgres
psql -h 127.0.0.1 -p 5432 -U temporal -d postgres
```
