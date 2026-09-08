-- Creates the Temporal visibility database on SQL Server 2019+.
-- The UTF-8 collation makes the VARCHAR search-attribute columns (populated
-- via CAST from JSON_VALUE output) hold arbitrary Unicode without data loss;
-- UTF-8 collations require SQL Server 2019+. CI (case-insensitive) matches
-- the main-store database and MySQL's default comparison semantics.
-- READ_COMMITTED_SNAPSHOT matches what the temporal-sql-tool's
-- create-database command configures.
-- NOTE: keep these as plain statements (no IF guards) so Temporal's
-- semicolon-based statement splitter can parse this file.
CREATE DATABASE temporal_visibility COLLATE Latin1_General_100_CI_AS_SC_UTF8;
ALTER DATABASE temporal_visibility SET READ_COMMITTED_SNAPSHOT ON;
