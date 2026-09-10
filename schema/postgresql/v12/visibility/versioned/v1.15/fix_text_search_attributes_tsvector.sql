-- The Text search attribute columns were defined as generated columns using a
-- raw ::tsvector cast: (search_attributes->>'TextNN')::tsvector. That cast
-- requires the value to be valid tsvector literal syntax, so any value
-- containing a character that is significant to tsvector (most notably ':')
-- raises "syntax error in tsvector" and fails the whole executions_visibility
-- upsert. Recompute the columns with to_tsvector('simple', ...) instead, which
-- tokenizes arbitrary text and never fails.
--
-- Dropping each generated column also drops its dependent GIN index, so the
-- indexes are recreated afterwards. to_tsvector never errors, so recomputing
-- the stored values for existing rows during the column re-add is safe.

ALTER TABLE executions_visibility DROP COLUMN Text01;
ALTER TABLE executions_visibility DROP COLUMN Text02;
ALTER TABLE executions_visibility DROP COLUMN Text03;

ALTER TABLE executions_visibility
  ADD COLUMN Text01 TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', search_attributes->>'Text01')) STORED;
ALTER TABLE executions_visibility
  ADD COLUMN Text02 TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', search_attributes->>'Text02')) STORED;
ALTER TABLE executions_visibility
  ADD COLUMN Text03 TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', search_attributes->>'Text03')) STORED;

CREATE INDEX by_text_01 ON executions_visibility USING GIN (namespace_id, Text01);
CREATE INDEX by_text_02 ON executions_visibility USING GIN (namespace_id, Text02);
CREATE INDEX by_text_03 ON executions_visibility USING GIN (namespace_id, Text03);
