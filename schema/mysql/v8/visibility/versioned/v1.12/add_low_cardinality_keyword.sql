ALTER TABLE chasm_search_attributes ADD COLUMN TemporalLowCardinalityKeyword01 VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.TemporalLowCardinalityKeyword01");
CREATE INDEX by_temporal_low_cardinality_keyword_01 ON chasm_search_attributes (namespace_id, TemporalLowCardinalityKeyword01);
