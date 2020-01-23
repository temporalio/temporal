CREATE TYPE checksum (version int, flavor int, value blob);
ALTER TABLE executions ADD checksum frozen<checksum>;