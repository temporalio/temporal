ALTER TABLE executions_visibility ADD COLUMN _version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE custom_search_attributes ADD COLUMN _version BIGINT NOT NULL DEFAULT 0;
