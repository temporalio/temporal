-- Creates the default Temporal main-store database on SQL Server 2019+.
-- The UTF-8 collation is the SQL Server analog of MySQL's utf8mb4 charset;
-- CI (case-insensitive) matches MySQL's default comparison semantics.
-- READ_COMMITTED_SNAPSHOT gives readers MVCC semantics comparable to the
-- READ COMMITTED behavior of MySQL/PostgreSQL and avoids reader/writer blocking.
-- NOTE: keep these as plain statements (no IF guards) so Temporal's
-- semicolon-based statement splitter can parse this file.
CREATE DATABASE temporal COLLATE Latin1_General_100_CI_AS_SC_UTF8;
ALTER DATABASE temporal SET READ_COMMITTED_SNAPSHOT ON;
