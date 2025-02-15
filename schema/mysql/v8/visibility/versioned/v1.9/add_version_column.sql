ALTER TABLE executions_visibility ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE custom_search_attributes ADD COLUMN version BIGINT NOT NULL DEFAULT 0;