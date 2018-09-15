ALTER TABLE domains_by_name ADD config_version bigint;
ALTER TABLE domains_by_name ADD is_global_domain boolean;